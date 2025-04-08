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

SECOND_BACKEND_URL = "http://localhost:8012/upload_2"

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
        if "creation_time" in metadata:
            try:
                dt = datetime.strptime(metadata["creation_time"].split(".")[0], "%Y-%m-%dT%H:%M:%S")
                dt = dt.replace(tzinfo=timezone.utc)
                metadata["creation_time_utc"] = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
                metadata["creation_time_local"] = dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
                metadata["recording_time"] = metadata["creation_time_local"]  # For backward compatibility
            except Exception:
                pass

        return metadata

    except ffmpeg.Error as e:
        print(f"FFmpeg error: {e.stderr.decode('utf-8')}")
        return None
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def predict_age_gender(face_img):
    """Predict age and gender using DeepFace"""
    try:
        analysis = DeepFace.analyze(face_img, actions=["age", "gender"], enforce_detection=False)
        return analysis[0]["dominant_gender"], analysis[0]["age"]
    except Exception:
        return "Unknown", "Unknown"

def is_carried(obj_bbox, person_bbox):
    """Check if an object is being carried by a person"""
    px1, py1, px2, py2 = person_bbox
    ox1, oy1, ox2, oy2 = obj_bbox
    obj_center_y = (oy1 + oy2) / 2
    lower_half_threshold = py1 + (py2 - py1) * 0.6
    overlap = (ox1 > px1) and (ox2 < px2) and (oy1 > py1) and (oy2 < py2)
    in_carry_position = obj_center_y > lower_half_threshold
    return overlap and in_carry_position

def calculate_speed(prev_position, current_position, fps, frame_interval=1):
    """
    Calculate speed in pixels per second between two positions
    frame_interval: number of frames between measurements (for smoothing)
    """
    if prev_position is None or current_position is None:
        return 0

    # Use center points for speed calculation
    prev_center = ((prev_position[0] + prev_position[2]) / 2, (prev_position[1] + prev_position[3]) / 2)
    curr_center = ((current_position[0] + current_position[2]) / 2, (current_position[1] + current_position[3]) / 2)

    # Calculate Euclidean distance
    distance = math.sqrt((curr_center[0] - prev_center[0])**2 + (curr_center[1] - prev_center[1])**2)

    # Convert to pixels per second
    speed = distance * fps / frame_interval
    return speed

def ModelRun(SOURCE_VIDEO_PATH, TARGET_VIDEO_PATH, points):

        # ===== Initialize =====
    total_count = 0
    entering_count = 0
    exiting_count = 0
    restricted_area_count = 0  # New counter for restricted area
    tracker_states = {}
    restricted_area_states = {}  # To track who entered restricted area
    detection_data = {}
    previous_positions = {}  # To track movement between frames

    model_path = os.path.join("Model", "yolov8x.pt")
    model = YOLO(model_path)  
    model.fuse() 

    # ===== Main Processing =====
    
    video_info = sv.VideoInfo.from_video_path(SOURCE_VIDEO_PATH)

    area1 = np.array([(1169, 1678+50), (1942, 2025+50), (1816, 2102+50), (1085, 1703+50)], np.int32)
    area2 = np.array([(1040, 1710+50), (1771, 2117+50), (1673, 2142+50), (981, 1713+50)], np.int32)
    restricted_area = np.array(points)

    video_name = os.path.splitext(os.path.basename(SOURCE_VIDEO_PATH))[0]
    json_output_path = os.path.join(RESULTS_FOLDER, f"{video_name}_frame_data.json")
    # video_metadata = extract_video_metadata(SOURCE_VIDEO_PATH)

    # Get recording time from metadata or use current time as fallback
    try:
        recording_time = datetime.now()
        recording_time = recording_time.replace(tzinfo=timezone.utc)
    except (KeyError, ValueError):
        print("Warning: Using current time as recording time fallback")
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

            # Process each person
            for person_bbox, confidence, tracker_id in people:
                x1, y1, x2, y2 = person_bbox
                bottom_right = (int(x2), int(y2))
                center = (int((x1 + x2) / 2), int((y1 + y2) / 2))

                # Calculate speed
                current_position = (x1, y1, x2, y2)
                prev_position = previous_positions.get(tracker_id, None)
                speed = calculate_speed(prev_position, current_position, video_info.fps) if frame_number > 0 else 0
                previous_positions[tracker_id] = current_position

                # Age/gender detection
                face_roi = frame[int(y1):int(y2), int(x1):int(x2)]
                gender, age = predict_age_gender(face_roi)

                # Check for carried items
                carried_items = []
                for obj_bbox, _, class_id in objects:
                    if is_carried(obj_bbox, person_bbox):
                        if class_id in BAG_CLASSES:
                            carried_items.append("bag")
                        elif class_id == CAT_CLASS:
                            carried_items.append("cat")
                        elif class_id == DOG_CLASS:
                            carried_items.append("dog")

                # Initialize or update person data
                if tracker_id not in tracker_states:
                    tracker_states[tracker_id] = []
                    restricted_area_states[tracker_id] = False
                    detection_data[tracker_id] = {
                        "tracker_id": int(tracker_id),
                        "gender": gender,
                        "age": age,
                        "carrying": carried_items if carried_items else "no objects",
                        "bbox_history": [],
                        "confidence": float(confidence),
                        "entered_restricted_area": False,
                        "restricted_area_entry_time": None,
                        "restricted_area_entry_frame": None
                    }

                # Check restricted area entry
                in_restricted_area = cv2.pointPolygonTest(restricted_area, center, False) >= 0
                if in_restricted_area and not restricted_area_states[tracker_id]:
                    restricted_area_states[tracker_id] = True
                    detection_data[tracker_id]["entered_restricted_area"] = True
                    detection_data[tracker_id]["restricted_area_entry_time"] = (
                        recording_time + timedelta(seconds=frame_number / video_info.fps)).strftime("%Y-%m-%d %H:%M:%S")
                    detection_data[tracker_id]["restricted_area_entry_frame"] = frame_number
                    restricted_area_count += 1
                elif not in_restricted_area and restricted_area_states[tracker_id]:
                    restricted_area_states[tracker_id] = False

                # Area crossing logic
                in_area1 = cv2.pointPolygonTest(area1, bottom_right, False) >= 0
                in_area2 = cv2.pointPolygonTest(area2, bottom_right, False) >= 0

                if in_area2 and "area2" not in tracker_states[tracker_id]:
                    tracker_states[tracker_id].append("area2")
                    if tracker_states[tracker_id] == ["area2"]:
                        entry_time = recording_time + timedelta(seconds=frame_number / video_info.fps)
                        detection_data[tracker_id]["entry_time"] = entry_time.strftime("%Y-%m-%d %H:%M:%S")
                        detection_data[tracker_id]["entry_frame"] = frame_number

                if in_area1 and "area1" not in tracker_states[tracker_id]:
                    tracker_states[tracker_id].append("area1")
                    if tracker_states[tracker_id] == ["area1"]:
                        exit_time = recording_time + timedelta(seconds=frame_number / video_info.fps)
                        detection_data[tracker_id]["exit_time"] = exit_time.strftime("%Y-%m-%d %H:%M:%S")
                        detection_data[tracker_id]["exit_frame"] = frame_number

                # Update counts
                if tracker_states[tracker_id] == ["area2", "area1"]:
                    entering_count += 1
                    tracker_states[tracker_id] = []
                elif tracker_states[tracker_id] == ["area1", "area2"]:
                    exiting_count += 1
                    tracker_states[tracker_id] = []

                total_count = entering_count + exiting_count

                # Store bounding box history
                detection_data[tracker_id]["bbox_history"].append({
                    "frame_number": int(frame_number),
                    "bbox": [float(x1), float(y1), float(x2), float(y2)],
                    "timestamp": (recording_time + timedelta(seconds=frame_number / video_info.fps)).strftime("%Y-%m-%d %H:%M:%S"),
                    "carrying": carried_items if carried_items else "no objects",
                    "speed": speed,

                    "in_restricted_area": in_restricted_area
                })

                # Draw bounding box and label
                carrying_str = ", ".join(carried_items) if carried_items else "no objects"
                label = f"ID: {tracker_id} | {gender}, {age} |Carrying: {carrying_str} | Speed: {speed:.2f} px/s"
                if in_restricted_area:
                    label += " | IN RESTRICTED AREA"
                    box_color = (0, 0, 255)  # Red for restricted area
                else:
                    box_color = (255, 0, 0)  # Blue for normal

                cv2.rectangle(frame, (int(x1), int(y1)), (int(x2), int(y2)), box_color, 3)
                cv2.putText(frame, label, (int(x1), int(y1) - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 2)

            # Draw counting areas and counters
            cv2.polylines(frame, [area1], isClosed=True, color=(255, 0, 0), thickness=2)
            cv2.polylines(frame, [area2], isClosed=True, color=(0, 255, 0), thickness=2)
            cv2.polylines(frame, [restricted_area], isClosed=True, color=(0, 0, 255), thickness=2)
            cv2.putText(frame, "Restricted Area", (restricted_area[0][0], restricted_area[0][1] - 10),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)

            # Display counters
            cv2.putText(frame, f"Total: {total_count}", (50, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
            cv2.putText(frame, f"Entering: {entering_count}", (50, 100), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)
            cv2.putText(frame, f"Exiting: {exiting_count}", (50, 150), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
            cv2.putText(frame, f"Restricted Area Entries: {restricted_area_count}", (50, 200),
                    cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 165, 255), 2)

            # Write frame to output video
            sink.write_frame(frame)

    # ===== Save Results =====
    json_output = {
        # "video_metadata": video_metadata,
        "processing_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z"),
        "summary": {
            "total_people": int(total_count),
            "total_entering": int(entering_count),
            "total_exiting": int(exiting_count),
            "restricted_area_entries": int(restricted_area_count),  # New field
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

            points = response_data.get("points", None)
            if points is None:
                return jsonify({"error": "Points not found in the response from the second backend"}), 400

            print("Received points from second backend:", points)

            try:
                points = json.loads(points)
            except json.JSONDecodeError as e:
                return jsonify({"error": f"Invalid JSON in points: {e}"}), 400

            print("Successfully parsed points:", points)

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
    app.run(host="0.0.0.0", port=8011, debug=True)