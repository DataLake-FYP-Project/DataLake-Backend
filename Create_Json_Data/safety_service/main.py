from flask import Flask, jsonify, request
import requests
import supervision as sv
from ultralytics import YOLO
import os
import json
import subprocess
import cv2  # OpenCV for image saving
import numpy as np
from sklearn.cluster import KMeans
import ffmpeg
from datetime import datetime, timedelta, timezone
from shapely.geometry import LineString, Point

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
RESULTS_FOLDER = "results"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)
FRAME_SAVE_DIR = 'results/frames/'

SECOND_BACKEND_URL = "http://localhost:8013/upload_2_vehicle"


# All classes in our custom model
classNames = [
    'Hardhat', 'Mask', 'NO-Hardhat', 'NO-Mask', 'NO-Safety Vest',
    'Person', 'Safety Cone', 'Safety Vest', 'machinery', 'vehicle'
]

SAFETY_CLASSES = {"Hardhat", "Mask", "Safety Vest"}
UNSAFE_CLASSES = {"NO-Hardhat", "NO-Mask", "NO-Safety Vest"}

# -------------------------------
# INITIAL SETUP
# -------------------------------
os.makedirs(FRAME_SAVE_DIR, exist_ok=True)
box_annotator = sv.BoxAnnotator(thickness=2, text_thickness=2, text_scale=1)

id_counter = 1
id_map = {}
frame_data_list = []


def get_class_name(class_id):
    return classNames[class_id] if class_id < len(classNames) else f"Class {class_id}"

def ModelRun(SOURCE_VIDEO_PATH, TARGET_VIDEO_PATH):

    model_path = os.path.join("Model", "ppe.pt")
    model = YOLO(model_path)  
    model.fuse() 
    
    video_info = sv.VideoInfo.from_video_path(SOURCE_VIDEO_PATH)
    video_name = os.path.splitext(os.path.basename(SOURCE_VIDEO_PATH))[0]
    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    json_output_path = os.path.join(RESULTS_FOLDER, f"{video_name}_{now}.json")

    with sv.VideoSink(TARGET_VIDEO_PATH, video_info) as sink:
        for frame_number, result in enumerate(
            model.track(
                source=SOURCE_VIDEO_PATH,
                tracker='bytetrack.yaml',
                show=False,
                stream=True,
                agnostic_nms=True,
                persist=True
            )
        ):
            frame = result.orig_img
            detections = sv.Detections.from_yolov8(result)

            if result.boxes.id is not None:
                ids = result.boxes.id.cpu().numpy().astype(int)
                class_ids = result.boxes.cls.cpu().numpy().astype(int)
                confs = result.boxes.conf.cpu().numpy()
                boxes = result.boxes.xyxy.cpu().numpy()

                detected = []

                for i in range(len(ids)):
                    tracker_id = ids[i]
                    class_id = class_ids[i]
                    confidence = float(confs[i])
                    bbox = list(map(int, boxes[i]))
                    class_name = get_class_name(class_id)

                    # Assign new sequential ID
                    if tracker_id not in id_map:
                        id_map[tracker_id] = id_counter
                        id_counter += 1
                    new_id = id_map[tracker_id]

                    detected.append({
                        "id": new_id,
                        "tracker_id": tracker_id,
                        "class_id": class_id,
                        "class_name": class_name,
                        "confidence": confidence,
                        "bbox": bbox
                    })
                # Separate people and other objects
                people = [d for d in detected if d["class_name"] == "Person"]
                objects = [d for d in detected if d["class_name"] != "Person"]

                people_info = []

                for person in people:
                    tid = person["tracker_id"]
                    new_id = id_map[tid]
                    person_bbox = person["bbox"]
                
                    # Initialize
                    person_state = {
                        "hardhat": None,
                        "mask": None,
                        "safety_vest": None
                    }

                    # Check which objects are near the person’s bounding box
                    for obj in objects:
                        obj_class = obj["class_name"]
                        obj_bbox = obj["bbox"]
                
                        # IoU or proximity check — here we use simple overlap
                        px1, py1, px2, py2 = person_bbox
                        ox1, oy1, ox2, oy2 = obj_bbox
                
                        is_near = not (ox2 < px1 or ox1 > px2 or oy2 < py1 or oy1 > py2)
                
                        if is_near:
                            if obj_class == "Hardhat":
                                person_state["hardhat"] = True
                            elif obj_class == "NO-Hardhat":
                                person_state["hardhat"] = False
                            elif obj_class == "Mask":
                                person_state["mask"] = True
                            elif obj_class == "NO-Mask":
                                person_state["mask"] = False
                            elif obj_class == "Safety Vest":
                                person_state["safety_vest"] = True
                            elif obj_class == "NO-Safety Vest":
                                person_state["safety_vest"] = False

                    # Determine safety status
                    missing_items = [item for item, value in person_state.items() if value != True]
                    status = "Safe" if not missing_items else "Unsafe"
                    color = (0, 255, 0) if status == "Safe" else (0, 0, 255)

                    label = f"ID {new_id} | {status}"
                    cv2.rectangle(frame, (person_bbox[0], person_bbox[1]), (person_bbox[2], person_bbox[3]), color, 2)
                    cv2.putText(frame, label, (person_bbox[0], person_bbox[1] - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.6, color, 2)
                
                    person_state.update({
                        "tracker_id": new_id,
                        "safety_status": status,
                        "missing_items": missing_items,
                        "bbox": person_bbox
                    })
                
                    people_info.append(person_state)

                # Save frame
                frame_path = os.path.join(FRAME_SAVE_DIR, f"frame_{frame_number:04d}.jpg")
                cv2.imwrite(frame_path, cv2.cvtColor(frame, cv2.COLOR_RGB2BGR))

                # Save JSON info
                frame_data_list.append({
                    "frame_number": frame_number,
                    "people": people_info
                })

                # Write annotated frame to video
                sink.write_frame(frame)

    print("Safety detection and video generation completed.")


    # Save frame data to a JSON file
    with open(json_output_path, 'w') as json_file:
        json.dump(frame_data_list, json_file, indent=4)

    return json_output_path


@app.route("/upload_safety", methods=["POST"])
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




if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8014, debug=True,use_reloader=False)