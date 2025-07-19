import requests
from flask import Flask, jsonify, request
import supervision as sv
from ultralytics import YOLO
import os
import json
from datetime import datetime

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
RESULTS_FOLDER = "results"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)
FRAME_SAVE_DIR = 'results/frames/'

SECOND_BACKEND_URL = "http://localhost:8013/upload_2_common"

os.makedirs(FRAME_SAVE_DIR, exist_ok=True)
box_annotator = sv.BoxAnnotator(thickness=2, text_thickness=2, text_scale=1)


def ModelRun(SOURCE_VIDEO_PATH, TARGET_VIDEO_PATH):
    model_path = os.path.join("Model", "yolov8n.pt")
    model = YOLO(model_path)
    model.fuse()

    video_info = sv.VideoInfo.from_video_path(SOURCE_VIDEO_PATH)
    video_name = os.path.splitext(os.path.basename(SOURCE_VIDEO_PATH))[0]
    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    json_output_path = os.path.join(RESULTS_FOLDER, f"{video_name}_{now}.json")

    all_detections = []

    with sv.VideoSink(TARGET_VIDEO_PATH, video_info) as sink:
        for frame_number, result in enumerate(
                model.track(
                    source=SOURCE_VIDEO_PATH,
                    tracker="bytetrack.yaml",
                    show=False,
                    stream=True,
                    agnostic_nms=True,
                    persist=True
                )
        ):
            frame = result.orig_img

            if result.boxes.id is not None:
                ids = result.boxes.id.cpu().numpy().astype(int)
                class_ids = result.boxes.cls.cpu().numpy().astype(int)
                confs = result.boxes.conf.cpu().numpy()
                boxes = result.boxes.xyxy.cpu().numpy()

                for i in range(len(ids)):
                    tracker_id = ids[i]
                    class_id = class_ids[i]
                    conf = float(confs[i])
                    bbox = list(map(int, boxes[i]))

                    all_detections.append({
                        "frame_number": frame_number,
                        "tracker_id": int(tracker_id),
                        "class_id": int(class_id),
                        "class_name": model.names[class_id],
                        "confidence": conf,
                        "bbox": bbox
                    })

            sink.write_frame(frame)

    # Save all detections
    with open(json_output_path, 'w') as f:
        json.dump(all_detections, f, indent=4)

    print(f"Object detection complete: {json_output_path}")
    return json_output_path


@app.route("/upload_common", methods=["POST"])
def upload_common_video():
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No selected file"}), 400

    source_video_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(source_video_path)

    target_video_path = os.path.join(RESULTS_FOLDER, "common_" + file.filename)

    try:
        json_output_path = ModelRun(source_video_path, target_video_path)

        try:
            with open(json_output_path, 'rb') as json_file:
                response = requests.post(SECOND_BACKEND_URL, files={"json_file": json_file}, timeout=10)
                response_data = response.json()

        except Exception as e:
            return jsonify({"error": f"Error during second backend communication: {str(e)}"}), 500

        return jsonify({
            "message": "Common object detection completed",
            "json_output": json_output_path,
            "second_backend_response": response_data
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8017, debug=True, use_reloader=False)
