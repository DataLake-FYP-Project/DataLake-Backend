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

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
RESULTS_FOLDER = "results"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)
FRAME_SAVE_DIR = "results/Frames"

SECOND_BACKEND_URL = "http://localhost:8013/upload_2_people"


# Object classes (COCO dataset)
BAG_CLASSES = [24, 26, 28]  # backpack, handbag, suitcase
CAT_CLASS = 15
DOG_CLASS = 16




# ===== Helper Functions =====
def extract_video_metadata(video_path):
    """Extract all available metadata from a video file using ffmpeg-python."""
    try:
        probe = ffmpeg.probe(video_path)
        metadata = {}

        # ===== 1. General Video Information =====
        if "format" in probe:
            format_info = probe["format"]
            metadata.update({
                "filename": format_info.get("filename"),
                "format_name": format_info.get("format_name"),
                "format_long_name": format_info.get("format_long_name"),
                "duration_seconds": float(format_info.get("duration", 0)),
                "size_bytes": int(format_info.get("size", 0)),
                "bitrate": int(format_info.get("bit_rate", 0)),
            })

            # Extract creation_time (if available)
            if "tags" in format_info:
                metadata.update({
                    "creation_time": format_info["tags"].get("creation_time"),
                    "encoder": format_info["tags"].get("encoder"),
                })

        # ===== 2. Video Stream Metadata =====
        video_streams = [s for s in probe["streams"] if s["codec_type"] == "video"]
        if video_streams:
            video_info = video_streams[0]
            metadata.update({
                "video_codec": video_info.get("codec_name"),
                "width": int(video_info.get("width", 0)),
                "height": int(video_info.get("height", 0)),
                "fps": eval(video_info.get("avg_frame_rate", "0/1")),  # e.g., "30/1" → 30.0
            })

            # Extract device-specific metadata (iPhone, Android, etc.)
            if "tags" in video_info:
                metadata.update({
                    "device_model": video_info["tags"].get("com.apple.quicktime.model"),
                    "software": video_info["tags"].get("software"),
                })

        # ===== 3. Audio Stream Metadata =====
        audio_streams = [s for s in probe["streams"] if s["codec_type"] == "audio"]
        if audio_streams:
            audio_info = audio_streams[0]
            metadata.update({
                "audio_codec": audio_info.get("codec_name"),
                "sample_rate": int(audio_info.get("sample_rate", 0)),
                "channels": int(audio_info.get("channels", 0)),
            })

        # ===== 4. GPS Coordinates (if recorded) =====
        if "format" in probe and "tags" in probe["format"]:
            tags = probe["format"]["tags"]
            if "location" in tags:  # Some Android devices store GPS here
                metadata["gps_coordinates"] = tags["location"]
            elif "com.apple.quicktime.location.ISO6709" in tags:  # iPhone GPS
                metadata["gps_coordinates"] = tags["com.apple.quicktime.location.ISO6709"]

        # ===== 5. Convert ISO Timestamp to Readable Format =====
        if "creation_time" in metadata and metadata["creation_time"]:
            try:
                # If creation_time exists, process it
                dt = datetime.strptime(metadata["creation_time"].split(".")[0], "%Y-%m-%dT%H:%M:%S")
                dt = dt.replace(tzinfo=timezone.utc)
                metadata["creation_time_utc"] = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
                metadata["creation_time_local"] = dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
                metadata["recording_time"] = metadata["creation_time_local"]  # For backward compatibility
            except Exception as e:
                # Log any exception
                print(f"Error processing creation_time: {e}")
                # Fallback to current time if there's an error
                dt = datetime.now(timezone.utc)
        else:
            # If creation_time is not found or is None, use the current time
            dt = datetime.now(timezone.utc)


        return metadata

    except ffmpeg.Error as e:
        print(f"FFmpeg error: {e.stderr.decode('utf-8')}")
        return None
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def is_carried(obj_bbox, person_bbox):
    """Check if an object is being carried by a person"""
    px1, py1, px2, py2 = person_bbox
    ox1, oy1, ox2, oy2 = obj_bbox
    obj_center_y = (oy1 + oy2) / 2
    lower_half_threshold = py1 + (py2 - py1) * 0.6
    overlap = (ox1 > px1) and (ox2 < px2) and (oy1 > py1) and (oy2 < py2)
    in_carry_position = obj_center_y > lower_half_threshold
    return overlap and in_carry_position

def analyze_person(frame, bbox, objects):
    """Analyze a person's attributes at entry/exit points"""
    x1, y1, x2, y2 = map(int, bbox)
    face_roi = frame[y1:y2, x1:x2]

    # Age/gender detection
    try:
        analysis = DeepFace.analyze(face_roi, actions=['age', 'gender'], enforce_detection=False)
        gender = analysis[0]['dominant_gender']
        age = analysis[0]['age']
    except:
        gender, age = "Unknown", "Unknown"

    # Check for carried items
    carried_items = []
    for obj_bbox, _, class_id in objects:
        if is_carried(obj_bbox, bbox):
            if class_id in BAG_CLASSES:
                carried_items.append("bag")
            elif class_id == CAT_CLASS:
                carried_items.append("cat")
            elif class_id == DOG_CLASS:
                carried_items.append("dog")

    return gender, age, carried_items if carried_items else "no objects"

def ModelRun(SOURCE_VIDEO_PATH, TARGET_VIDEO_PATH, points):

    total_count = 0
    entering_count = 0
    exiting_count = 0
    restricted_area_count = 0  # New counter for restricted area
    tracker_states = {}  # Tracks area crossings
    detection_data = {}  # Stores entry/exit information
    restricted_people = set()  # Track people who entered restricted area

    model_path = os.path.join("Model", "yolov8x.pt")
    model = YOLO(model_path)  
    model.fuse() 

    # ===== Main Processing =====
    
    video_info = sv.VideoInfo.from_video_path(SOURCE_VIDEO_PATH)

    restricted_area = np.array(points)
    area1 = np.array([(265, 427), (294, 423), (478, 514), (433, 523)], np.int32)
    area2 = np.array([(243, 446), (274, 437), (403, 510), (367, 521)], np.int32)


    video_name = os.path.splitext(os.path.basename(SOURCE_VIDEO_PATH))[0]
    json_output_path = os.path.join(RESULTS_FOLDER, f"people_{video_name}_frame_data.json")
    video_metadata = extract_video_metadata(SOURCE_VIDEO_PATH)

    # Get recording time from metadata or use current time as fallback
    if "creation_time" in video_metadata and video_metadata["creation_time"]:
        try:
            # Attempt to split and convert creation_time to a datetime object
            recording_time = datetime.strptime(video_metadata["creation_time"].split(".")[0], "%Y-%m-%dT%H:%M:%S")
            recording_time = recording_time.replace(tzinfo=timezone.utc)
        except (ValueError, AttributeError) as e:
            print(f"Error processing 'creation_time': {e}. Using current time as fallback.")
            recording_time = datetime.now(timezone.utc)
    else:
        print("No 'creation_time' found. Using current time as fallback.")
        recording_time = datetime.now(timezone.utc)

        
    with sv.VideoSink(TARGET_VIDEO_PATH, video_info) as sink:
        for frame_number, result in enumerate(
            model.track(source=SOURCE_VIDEO_PATH, tracker="bytetrack.yaml", show=False, stream=True, persist=True)
        ):
            frame = result.orig_img
            detections = sv.Detections.from_yolov8(result)

            # Separate detections
            people = []
            objects = []
            if result.boxes.id is not None:
                tracker_ids = result.boxes.id.cpu().numpy().astype(int)
                for i, (bbox, conf, class_id) in enumerate(zip(detections.xyxy, detections.confidence, detections.class_id)):
                    if class_id == 0:  # Person
                        tracker_id = tracker_ids[i] if i < len(tracker_ids) else None
                        people.append((bbox, conf, tracker_id))
                    elif class_id in BAG_CLASSES + [CAT_CLASS, DOG_CLASS]:
                        objects.append((bbox, conf, class_id))

            # Process each person's area crossings
            for bbox, conf, tracker_id in people:
                x1, y1, x2, y2 = bbox
                bottom_center = (int((x1+x2)/2), int(y2))

                # Check area crossings
                in_area1 = cv2.pointPolygonTest(area1, bottom_center, False) >= 0
                in_area2 = cv2.pointPolygonTest(area2, bottom_center, False) >= 0
                in_restricted = cv2.pointPolygonTest(restricted_area, bottom_center, False) >= 0

                current_area = None
                if in_area1: current_area = "area1"
                elif in_area2: current_area = "area2"

                # Initialize tracker only when entering monitored areas
                if tracker_id not in tracker_states and current_area:
                    tracker_states[tracker_id] = {
                        'current_area': current_area,
                        'last_area': None,
                        'entry_time': None,
                        'entered_restricted': False
                    }
                    detection_data[tracker_id] = {
                        "tracker_id": int(tracker_id),
                        "gender": "Unknown",
                        "age": "Unknown",
                        "carrying": "none",
                        "confidence": float(conf),
                        "entry_time": None,
                        "exit_time": None,
                        "entry_frame": None,
                        "exit_frame": None,
                        "entered_restricted": False,
                        "restricted_entry_time": None,
                        "restricted_exit_time": None
                    }

                # Only process if person is in our tracking system (entered monitored area)
                if tracker_id in tracker_states:
                    # Check restricted area entry
                    if in_restricted and not tracker_states[tracker_id]['entered_restricted']:
                        tracker_states[tracker_id]['entered_restricted'] = True
                        detection_data[tracker_id]['entered_restricted'] = True
                        detection_data[tracker_id]['restricted_entry_time'] = (
                            recording_time + timedelta(seconds=frame_number / video_info.fps)
                        ).strftime("%Y-%m-%d %H:%M:%S")
                        restricted_area_count += 1
                        restricted_people.add(tracker_id)

                        # Draw alert for restricted area entry
                        cv2.putText(frame, "RESTRICTED AREA ENTRY!", (int(x1), int(y1)-30),
                                cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)

                    # Check restricted area exit
                    elif not in_restricted and tracker_states[tracker_id]['entered_restricted']:
                        detection_data[tracker_id]['restricted_exit_time'] = (
                            recording_time + timedelta(seconds=frame_number / video_info.fps)
                        ).strftime("%Y-%m-%d %H:%M:%S")

                    # Handle area transitions
                    if current_area != tracker_states[tracker_id]['current_area']:
                        # Entry event (area2)
                        if current_area == "area1" and tracker_states[tracker_id]['last_area'] == "area2":
                            # Analyze person attributes
                            gender, age, carrying = analyze_person(frame, bbox, objects)

                            # Update detection data
                            entry_time = recording_time + timedelta(seconds=frame_number / video_info.fps)
                            detection_data[tracker_id].update({
                                "tracker_id": int(tracker_id),
                                "gender": gender,
                                "age": age,
                                "carrying": carrying,
                                "entry_time": entry_time.strftime("%Y-%m-%d %H:%M:%S"),
                                "entry_frame": frame_number,
                            })
                            entering_count += 1
                            total_count += 1

                        # Exit event (area1 after area2)
                        elif current_area == "area2" and tracker_states[tracker_id]['last_area'] == "area1":
                            # Analyze person attributes again at exit
                            gender, age, carrying = analyze_person(frame, bbox, objects)

                            # Update detection data
                            exit_time = recording_time + timedelta(seconds=frame_number / video_info.fps)
                            detection_data[tracker_id].update({
                                "tracker_id": int(tracker_id),
                                "gender": gender,
                                "age": age,
                                "carrying": carrying,
                                "exit_time": exit_time.strftime("%Y-%m-%d %H:%M:%S"),
                                "exit_frame": frame_number,
                            })
                            exiting_count += 1
                            total_count += 1

                        # Update tracker state
                        tracker_states[tracker_id]['last_area'] = tracker_states[tracker_id]['current_area']
                        tracker_states[tracker_id]['current_area'] = current_area

                    # Draw bounding box if person is being tracked
                    if detection_data[tracker_id]['entry_time']:
                        person = detection_data[tracker_id]
                        label = f"ID: {tracker_id} | {person['gender']}, {person['age']}"
                        if person['entered_restricted']:
                            # Highlight people who entered restricted area
                            cv2.rectangle(frame, (int(x1), int(y1)), (int(x2), int(y2)), (0, 0, 255), 3)
                            label += " | RESTRICTED"
                        else:
                            cv2.rectangle(frame, (int(x1), int(y1)), (int(x2), int(y2)), (0, 255, 0), 2)
                        cv2.putText(frame, label, (int(x1), int(y1)-10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255,255,255), 1)

            # Draw counters and areas
            cv2.polylines(frame, [area1], isClosed=True, color=(255, 0, 0), thickness=2)
            cv2.polylines(frame, [area2], isClosed=True, color=(0, 255, 0), thickness=2)
            cv2.polylines(frame, [restricted_area], isClosed=True, color=(0, 0, 255), thickness=2)
            cv2.putText(frame, "RESTRICTED AREA", (restricted_area[0][0], restricted_area[0][1]-10),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)

            cv2.putText(frame, f"Total: {total_count}", (50, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
            cv2.putText(frame, f"Entering: {entering_count}", (50, 100), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)
            cv2.putText(frame, f"Exiting: {exiting_count}", (50, 150), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
            cv2.putText(frame, f"Restricted Area: {restricted_area_count}", (50, 200),
                    cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)

            sink.write_frame(frame)

    # ===== Save Results =====
    json_output = {
        "video_metadata": video_metadata,
        "processing_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z"),
        "summary": {
            "total_people": int(total_count),
            "total_entering": int(entering_count),
            "total_exiting": int(exiting_count),
            "restricted_area_entries": int(restricted_area_count),
            "restricted_people_ids": [int(id) for id in restricted_people],
            "fps": float(video_info.fps),
            "duration_seconds": float(video_info.total_frames / video_info.fps)
        },
        "detections": {
            int(tracker_id): data for tracker_id, data in detection_data.items()
        }
    }

    with open(json_output_path, "w") as f:
        json.dump(json_output, f, indent=4)
    return json_output_path

@app.route("/upload_people", methods=["POST"])
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
        print(f"SOURCE_VIDEO_PATH: {source_video_path}")
        print(f"TARGET_VIDEO_PATH: {target_video_path}")
        print(f"FRAME_SAVE_DIR: {FRAME_SAVE_DIR}")


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
    app.run(host="0.0.0.0", port=8011, debug=True, use_reloader=False)