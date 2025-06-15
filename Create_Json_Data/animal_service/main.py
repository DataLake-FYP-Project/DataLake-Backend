import math
from flask import Flask, request, jsonify
import os
import supervision as sv
from ultralytics import YOLO
import json
import cv2
import numpy as np
import requests
from deepface import DeepFace
import ffmpeg
from datetime import timezone
from datetime import datetime, timedelta
import time
from collections import defaultdict

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
RESULTS_FOLDER = "results"
FRAME_SAVE_DIR = os.path.join(RESULTS_FOLDER, "Frames")
SECOND_BACKEND_URL = "http://localhost:8013/upload_2_animal"
MODEL_DIR = 'Model'

PROTO_TXT = os.path.join(MODEL_DIR, "MobileNetSSD_deploy.prototxt.txt")
CAFFE_MODEL = os.path.join(MODEL_DIR, "MobileNetSSD_deploy.caffemodel")

CLASSES = [
    "background", "aeroplane", "bicycle", "bird", "boat", "bottle",
    "bus", "car", "cat", "chair", "cow", "dining-table", "dog",
    "horse", "motorbike", "person", "potted plant", "sheep",
    "sofa", "train", "monitor"
]
REQ_CLASSES = ["bird", "cat", "cow", "dog", "horse", "sheep"]
COLORS = np.random.uniform(0, 255, size=(len(CLASSES), 3))
CONF_THRESH = 0.2

# Create folders
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)
os.makedirs(FRAME_SAVE_DIR, exist_ok=True)
os.makedirs(MODEL_DIR, exist_ok=True)

# ===== Helper Functions =====

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


# ===== ModelRun Function =====

def ModelRun(SOURCE_VIDEO_PATH, TARGET_VIDEO_PATH):  
    video_name = os.path.splitext(os.path.basename(SOURCE_VIDEO_PATH))[0]
    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    json_output_path = os.path.join(RESULTS_FOLDER, f"{video_name}_{now}.json")

    net = cv2.dnn.readNetFromCaffe(PROTO_TXT, CAFFE_MODEL)

    vs = cv2.VideoCapture(SOURCE_VIDEO_PATH)
    if not vs.isOpened():
        raise ValueError(f"Error opening video file: {SOURCE_VIDEO_PATH}")

    frame_width = int(vs.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(vs.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = int(vs.get(cv2.CAP_PROP_FPS))
    total_frames = int(vs.get(cv2.CAP_PROP_FRAME_COUNT))

    #Initialize video writer to save processed video
    out = cv2.VideoWriter(
        TARGET_VIDEO_PATH,
        cv2.VideoWriter_fourcc(*'mp4v'),
        fps,
        (frame_width, frame_height)
    )

    frame_data_list = []

    print(f"Processing video: {SOURCE_VIDEO_PATH}")
    print(f"Video dimensions: {frame_width}x{frame_height}, Total frames: {total_frames}")

    while True:
        start_time = time.time()
        success, frame = vs.read()
        if not success:
            break

        frame_number = int(vs.get(cv2.CAP_PROP_POS_FRAMES))
        timestamp_sec = round(float(vs.get(cv2.CAP_PROP_POS_MSEC)) / 1000, 2)

        (h, w) = frame.shape[:2]
        blob = cv2.dnn.blobFromImage(cv2.resize(frame, (300, 300)), 0.007843, (300, 300), 127.5)
        net.setInput(blob)
        detections = net.forward()

        detected_animals = []

        for i in np.arange(0, detections.shape[2]):
            confidence = detections[0, 0, i, 2]
            if confidence > CONF_THRESH:
                idx = int(detections[0, 0, i, 1])
                if CLASSES[idx] in REQ_CLASSES:
                    box = detections[0, 0, i, 3:7] * np.array([w, h, w, h])
                    (startX, startY, endX, endY) = box.astype(int)

                    detection_info = {
                        "class_id": int(idx),
                        "class_name": CLASSES[idx],
                        "confidence": float(confidence),
                        "bbox": [int(startX), int(startY), int(endX), int(endY)],
                        "center": {
                            "x": float((startX + endX) / 2),
                            "y": float((startY + endY) / 2)
                        },
                        "area": int((endX - startX) * (endY - startY)),
                        "frame_number": int(frame_number),
                        "timestamp": float(timestamp_sec)
                    }
                    detected_animals.append(detection_info)

                    # Draw bounding box
                    label = f"{CLASSES[idx]}: {confidence * 100:.2f}%"
                    cv2.rectangle(frame, (startX, startY), (endX, endY), COLORS[idx], 2)
                    cv2.putText(frame, label, (startX, startY - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, COLORS[idx], 2)

        processing_time = (time.time() - start_time) * 1000
        detection_str = format_detection_counts(detected_animals) if detected_animals else "No animals detected"
        print(f"Frame {frame_number}/{total_frames} - {detection_str} ({processing_time:.1f}ms)")

        if detected_animals:
            frame_data_list.append({
                "frame_number": int(frame_number),
                "timestamp": float(timestamp_sec),
                "detections": detected_animals
            })
            frame_path = os.path.join(FRAME_SAVE_DIR, f"frame_{frame_number:04d}.jpg")
            cv2.imwrite(frame_path, frame)

        #Save frame to output video
        out.write(frame)

    vs.release()
    out.release()

    #Save JSON
    save_json(frame_data_list, json_output_path)

    print("Detection completed. Output video and JSON saved.")
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
