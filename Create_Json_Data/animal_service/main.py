import os
import json
import time
from urllib import request
import cv2
from flask import Flask, jsonify,request
import numpy as np
from datetime import datetime
from ultralytics import YOLO
from collections import defaultdict
from flask import Flask, jsonify, request
import requests
from datetime import datetime, timedelta, timezone


# Paths and folders
app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
RESULTS_FOLDER = "results"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)
FRAME_SAVE_DIR = 'results/frames/'

SECOND_BACKEND_URL = "http://localhost:8013/upload_2_animal"

# Required animal classes from COCO
REQ_CLASSES = ["cat", "dog", "horse", "sheep", "cow", "elephant", "bear", "zebra", "giraffe", "bird"]

# Helper functions
def convert_to_serializable(obj):
    if isinstance(obj, (np.integer, np.int32, np.int64)):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj

def save_json(data, path):
    serializable_data = json.loads(json.dumps(data, default=convert_to_serializable))
    with open(path, 'w') as f:
        json.dump(serializable_data, f, indent=4)

def format_detection_counts(detections):
    count_dict = defaultdict(int)
    for det in detections:
        count_dict[det['class_name']] += 1
    return ", ".join(f"{cnt} {cls}{'s' if cnt > 1 else ''}" for cls, cnt in count_dict.items())

# Main detection function
def ModelRun(SOURCE_VIDEO_PATH, TARGET_VIDEO_PATH):
    os.makedirs(FRAME_SAVE_DIR, exist_ok=True)
    model_path = os.path.join("Model", "yolov8x.pt")
    model = YOLO(model_path)
    model.fuse()
    
    # Get COCO class names from model
    COCO_CLASSES = model.names
    COLORS = np.random.uniform(0, 255, size=(len(COCO_CLASSES), 3))

    video_name = os.path.splitext(os.path.basename(SOURCE_VIDEO_PATH))[0]
    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    json_output_path = os.path.join(RESULTS_FOLDER, f"{video_name}_{now}.json")

    cap = cv2.VideoCapture(SOURCE_VIDEO_PATH)
    if not cap.isOpened():
        raise ValueError(f"Could not open video {SOURCE_VIDEO_PATH}")

    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = int(cap.get(cv2.CAP_PROP_FPS))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    out = cv2.VideoWriter(TARGET_VIDEO_PATH, cv2.VideoWriter_fourcc(*'mp4v'), fps, (frame_width, frame_height))
    frame_data_list = []

    frame_number = 0
    while True:
        ret, frame = cap.read()
        if not ret:
            break

        start_time = time.time()
        results = model(frame)[0]  # Ultralytics prediction
        detected_animals = []

        for box in results.boxes:
            cls_id = int(box.cls)
            cls_name = COCO_CLASSES[cls_id]
            conf = float(box.conf)

            if cls_name in REQ_CLASSES:
                x1, y1, x2, y2 = map(int, box.xyxy[0])
                label = f"{cls_name}: {conf * 100:.1f}%"
                cv2.rectangle(frame, (x1, y1), (x2, y2), COLORS[cls_id], 2)
                cv2.putText(frame, label, (x1, y1 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, COLORS[cls_id], 2)

                detected_animals.append({
                    "class_id": cls_id,
                    "class_name": cls_name,
                    "confidence": conf,
                    "bbox": [x1, y1, x2, y2],
                    "center": {"x": float((x1 + x2) / 2), "y": float((y1 + y2) / 2)},
                    "area": int((x2 - x1) * (y2 - y1)),
                    "frame_number": frame_number,
                    "timestamp": float(cap.get(cv2.CAP_PROP_POS_MSEC)) / 1000
                })

        detection_str = format_detection_counts(detected_animals) if detected_animals else "No animals detected"
        print(f"Frame {frame_number}/{total_frames}: {detection_str} ({(time.time() - start_time)*1000:.1f}ms)")

        if detected_animals:
            frame_data_list.append({
                "frame_number": frame_number,
                "timestamp": float(cap.get(cv2.CAP_PROP_POS_MSEC)) / 1000,
                "detections": detected_animals
            })

            frame_path = os.path.join(FRAME_SAVE_DIR, f"frame_{frame_number:04d}.jpg")
            cv2.imwrite(frame_path, frame)

        out.write(frame)
        frame_number += 1

    cap.release()
    out.release()

    save_json(frame_data_list, json_output_path)
    print(f"YOLOv8 detection completed. JSON saved at: {json_output_path}")
    return json_output_path


# ===== Flask Upload Endpoint =====

@app.route("/upload_animal", methods=["POST"])
def upload_video():
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No selected file"}), 400

    source_video_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(source_video_path)

    target_video_path = os.path.join(RESULTS_FOLDER, "processed_" + file.filename)

    try:
        print(f"SOURCE_VIDEO_PATH: {source_video_path}")
        print(f"TARGET_VIDEO_PATH: {target_video_path}")
        print(f"FRAME_SAVE_DIR: {FRAME_SAVE_DIR}")

        # Run model
        json_output_path = ModelRun(source_video_path, target_video_path)
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


# ===== Run Flask App =====

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8016, debug=True, use_reloader=False)
