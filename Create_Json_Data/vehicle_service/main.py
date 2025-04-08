from flask import Flask, request, jsonify
import os
import supervision as sv
from ultralytics import YOLO
import json
import cv2
import numpy as np
import requests  

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
RESULTS_FOLDER = "results"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)
FRAME_SAVE_DIR = "results/Frames"

SECOND_BACKEND_URL = "http://localhost:8013/upload_2"

def is_in_target_polygon(center_x, center_y, polygon):
    point = (center_x, center_y)
    return cv2.pointPolygonTest(np.array(polygon, dtype=np.int32), point, False) >= 0


def ModelRun(SOURCE_VIDEO_PATH, TARGET_VIDEO_PATH, points):
    model_path = os.path.join("Model", "yolov8x.pt")
    model = YOLO(model_path)  
    model.fuse() 

    SOURCE = np.array(points)

    box_annotator = sv.BoxAnnotator(
        thickness=4,
        text_thickness=4,
        text_scale=2
    )

    id_counter = 1
    id_map = {} 
    frame_data_list = []  

    video_info = sv.VideoInfo.from_video_path(SOURCE_VIDEO_PATH)
    generator = sv.video.get_video_frames_generator(SOURCE_VIDEO_PATH)

    # Extract the video name (without extension) for the JSON filename
    video_name = os.path.splitext(os.path.basename(SOURCE_VIDEO_PATH))[0]
    json_output_path = os.path.join(RESULTS_FOLDER, f"{video_name}_frame_data.json")

    with sv.VideoSink(TARGET_VIDEO_PATH, video_info) as sink:  
        tracking_model_path = os.path.join("Model", "yolov8x.pt")
        tracker = YOLO(tracking_model_path).track(
            source=SOURCE_VIDEO_PATH, 
            tracker='bytetrack.yaml', 
            show=False, 
            stream=True, 
            agnostic_nms=True, 
            persist=True
        )

        for frame_number, result in enumerate(tracker):
            frame = result.orig_img
            detections = sv.Detections.from_yolov8(result)

            if result.boxes.id is not None:
                for tracker_id in result.boxes.id.cpu().numpy().astype(int):
                    if tracker_id not in id_map:
                        id_map[tracker_id] = id_counter
                        id_counter += 1

                detections.tracker_id = [id_map[tracker_id] for tracker_id in result.boxes.id.cpu().numpy().astype(int)]

            # Only keep detections that are inside the polygon
            filtered_detections = []
            filtered_labels = []
            for bbox, confidence, class_id, tracker_id in detections:
                center_x = (bbox[0] + bbox[2]) / 2  # center x of the bounding box
                center_y = (bbox[1] + bbox[3]) / 2  # center y of the bounding box

                # Check if the center of the bounding box is inside the polygon
                if is_in_target_polygon(center_x, center_y, points):
                    filtered_detections.append((bbox, confidence, class_id, tracker_id))
                    filtered_labels.append(
                        f"ID: {tracker_id} | {model.model.names[class_id]} {confidence:0.2f}"
                    )

            # Only annotate the filtered detections
            if filtered_detections:
                frame = box_annotator.annotate(scene=frame, detections=filtered_detections, labels=filtered_labels)

                # Collect frame data for JSON
                frame_data = {
                    "frame_number": frame_number,
                    "detections": [
                        {
                            "tracker_id": int(tracker_id), 
                            "class_id": int(class_id),     
                            "confidence": float(confidence),
                            "bbox": [float(coord) for coord in bbox] 
                        }
                        for bbox, confidence, class_id, tracker_id in filtered_detections
                    ]
                }
                frame_data_list.append(frame_data)

            cv2.polylines(frame, [SOURCE.astype(np.int32)], isClosed=True, color=(0, 255, 0), thickness=5)
            
            sink.write_frame(frame)

    with open(json_output_path, 'w') as json_file:
        json.dump(frame_data_list, json_file, indent=4)

    return json_output_path


@app.route("/upload", methods=["POST"])
def upload_video():
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No selected file"}), 400
    points = request.form.get("points", None)
    
    if points:
        try:
            points = json.loads(points)
        except json.JSONDecodeError as e:
            return jsonify({"error": f"Invalid JSON in points: {e}"}), 400
    else:
        points = []

    source_video_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(source_video_path)

    target_video_path = os.path.join(RESULTS_FOLDER, "processed_" + file.filename)
    
    try:
        print(f"Source Video Path: {source_video_path}")
        print(f"Target Video Path: {target_video_path}")

        json_output_path = ModelRun(source_video_path, target_video_path, points)
        print(f"JSON Output Path: {json_output_path}")

    except Exception as e:
        print(f"Error in ModelRun: {str(e)}")
        return jsonify({"error": f"Error processing video: {str(e)}"}), 500

    try:
        with open(json_output_path, 'rb') as json_file:
            response = requests.post(SECOND_BACKEND_URL, files={"json_file": json_file}, timeout=10)
            response_data = response.json()

    except Exception as e:
        return jsonify({"error": f"Error during second backend communication: {str(e)}"}), 500

    return jsonify({
        "message": "File uploaded and processed successfully",
        "source_video": source_video_path,
        "processed_video": target_video_path,
        "json_output": json_output_path,
        "second_backend_response": response_data
    }), 200




if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8012, debug=True)