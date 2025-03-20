from flask import Flask, request, jsonify
import os
import supervision as sv
from ultralytics import YOLO
import os
import json
import cv2
import numpy as np

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
RESULTS_FOLDER = "results"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)
FRAME_SAVE_DIR = "Results/Frames"


def ModelRun(SOURCE_VIDEO_PATH, TARGET_VIDEO_PATH):
    # Load the detection model (yolov8x.pt) from the Model folder
    model_path = os.path.join("Model", "yolov8x.pt")
    model = YOLO(model_path)  
    model.fuse()  # Perform model fusion if required for faster inference

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

    with sv.VideoSink(TARGET_VIDEO_PATH, video_info) as sink:  
        # Load the tracking model (yolov8s.pt) from the Model folder
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
            # Extract frame and detections
            frame = result.orig_img
            detections = sv.Detections.from_yolov8(result)

            # Handle object IDs (tracker IDs)
            if result.boxes.id is not None:
                for tracker_id in result.boxes.id.cpu().numpy().astype(int):
                    # Assign sequential ID if not already assigned
                    if tracker_id not in id_map:
                        id_map[tracker_id] = id_counter
                        id_counter += 1

                # Update detections with new sequential IDs
                detections.tracker_id = [id_map[tracker_id] for tracker_id in result.boxes.id.cpu().numpy().astype(int)]

            # Define labels for each detection
            labels = [
                f"ID: {tracker_id} | {model.model.names[class_id]} {confidence:0.2f}"
                for bbox, confidence, class_id, tracker_id in detections
            ]

            # Annotate the frame
            frame = box_annotator.annotate(scene=frame, detections=detections, labels=labels)

            # Save current frame to disk
            frame_path = os.path.join(FRAME_SAVE_DIR, f"frame_{frame_number:04d}.jpg")
            cv2.imwrite(frame_path, cv2.cvtColor(frame, cv2.COLOR_RGB2BGR))  # Convert RGB to BGR for OpenCV

            # Collect frame data for JSON
            frame_data = {
                "frame_number": frame_number,
                "detections": [
                    {
                        "tracker_id": int(tracker_id),  # Convert to Python int
                        "class_id": int(class_id),      # Convert to Python int
                        "confidence": float(confidence),  # Convert to Python float
                        "bbox": [float(coord) for coord in bbox]  # Convert bbox to list of floats
                    }
                    for bbox, confidence, class_id, tracker_id in detections
                ]
            }
            frame_data_list.append(frame_data)

            # Write annotated frame to the output video
            sink.write_frame(frame)

        # Save frame data as a JSON file
    json_output_path = "frame_data.json"  # Specify your desired path for the JSON file
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
    
    # Ensure the directory exists before saving the file
    source_video_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(source_video_path)

    # Run the model function and get the paths for processed video and JSON output
    target_video_path = os.path.join(RESULTS_FOLDER, "processed_" + file.filename)
    json_output_path = os.path.join(RESULTS_FOLDER, "output.json")

    # Call ModelRun function, which will generate the processed video and JSON output
    json_output_path = ModelRun(source_video_path, target_video_path)  # Now ModelRun will return the JSON file path

    return jsonify({
        "message": "File uploaded and processed successfully",
        "source_video": source_video_path,
        "processed_video": target_video_path,
        "json_output": json_output_path  # This will now be the correct JSON file path
    }), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8011, debug=True)
