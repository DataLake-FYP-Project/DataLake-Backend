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

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
RESULTS_FOLDER = "results"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)
FRAME_SAVE_DIR = 'results/frames/'

SECOND_BACKEND_URL = "http://localhost:8013/upload_2_vehicle"
SCALE_FACTOR = 0.05  # Conversion factor from pixels/frame to real-world speed (km/h)
VEHICLE_POSITIONS = {}
FPS = 30

def is_in_target_polygon(center_x, center_y, polygon):
    point = (center_x, center_y)
    return cv2.pointPolygonTest(np.array(polygon, dtype=np.int32), point, False) >= 0

# Predefined color names with RGB values (you can expand this list with more colors)
color_dict = {
    "red": (255, 0, 0),
    "green": (0, 255, 0),
    "blue": (0, 0, 255),
    "yellow": (255, 255, 0),
    "purple": (128, 0, 128),
    "orange": (255, 165, 0),
    "white": (255, 255, 255),
    "black": (0, 0, 0),
    "gray": (128, 128, 128),
    "brown": (139, 69, 19),
    "pink": (255, 192, 203),
    "violet": (238, 130, 238),
    "light_red": (255, 102, 102),  # Added more shades
    "light_green": (102, 255, 102),
    "light_blue": (102, 102, 255),
    "light_yellow": (255, 255, 102),
}

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

        
# Function to get the closest color name based on RGB values
def closest_color(rgb):
    min_colors = {}
    for name, color in color_dict.items():
        distance = np.linalg.norm(np.array(color) - np.array(rgb))
        min_colors[name] = distance
    return min(min_colors, key=min_colors.get)

def get_exact_vehicle_color(bbox, frame, k=1):
    # Crop the region of interest (vehicle) from the frame
    x1, y1, x2, y2 = map(int, bbox)
    vehicle_roi = frame[y1:y2, x1:x2]

    # Reshape the image into a 2D array of pixels
    pixels = vehicle_roi.reshape(-1, 3)

    # Apply KMeans clustering to find the dominant color
    kmeans = KMeans(n_clusters=k, n_init=10)
    kmeans.fit(pixels)

    # Get the dominant color (the centroid of the largest cluster)
    dominant_color = kmeans.cluster_centers_.astype(int)[0]

    # Convert BGR to RGB (OpenCV uses BGR by default)
    dominant_color_rgb = (dominant_color[2], dominant_color[1], dominant_color[0])  # BGR to RGB

    # Get the closest color name from the predefined dictionary
    closest_color_name = closest_color(dominant_color_rgb)

    return closest_color_name

# Function to calculate speed based on vehicle positions
def calculate_speed(center_x, center_y, tracker_id, frame_number, scale_factor=SCALE_FACTOR, fps=FPS):
    if tracker_id not in VEHICLE_POSITIONS:
        VEHICLE_POSITIONS[tracker_id] = (center_x, center_y, frame_number)
    
    prev_x, prev_y, prev_frame = VEHICLE_POSITIONS[tracker_id]
    displacement = np.sqrt((center_x - prev_x) ** 2 + (center_y - prev_y) ** 2)
    time_elapsed = (frame_number - prev_frame) / fps
    speed = (displacement / time_elapsed) * scale_factor if time_elapsed > 0 else 0
    
    # Update the vehicle's last known position
    VEHICLE_POSITIONS[tracker_id] = (center_x, center_y, frame_number)
    
    return speed

def is_stopped(speed):
    # If speed is 0, the vehicle is stopped
    return speed == 0

def get_vehicle_direction(center_x, center_y, tracker_id, vehicle_positions):
    direction = "Unknown"
    if tracker_id in vehicle_positions:
        prev_x, prev_y = vehicle_positions[tracker_id]
        
        # Compare Y movement to determine vertical direction
        if center_y > prev_y:
            direction = "Down"
        elif center_y < prev_y:
            direction = "Up"

    return direction

def get_lane(center_x, left_lane_end, right_lane_start):
    if center_x < left_lane_end:
        return "Left Lane"
    elif center_x > right_lane_start:
        return "Right Lane"
    else:
        return "Middle Lane"


def calculate_congestion_level(detections):
    return len(detections)

def handle_tracker_ids(tracker_ids, id_map, id_counter):
    updated_ids = []
    for tracker_id in tracker_ids:
        if tracker_id not in id_map:
            id_map[tracker_id] = id_counter
            id_counter += 1
        updated_ids.append(id_map[tracker_id])
    return updated_ids, id_map, id_counter

def ModelRun(SOURCE_VIDEO_PATH, TARGET_VIDEO_PATH, points):
    model_path = os.path.join("Model", "yolov8x.pt")
    model = YOLO(model_path)  
    model.fuse() 

    SOURCE = np.array(points)
    TARGET_WIDTH = 25
    TARGET_HEIGHT = 250

    TARGET = np.array([
        [0, 0],
        [TARGET_WIDTH - 1, 0],
        [TARGET_WIDTH - 1, TARGET_HEIGHT - 1],
        [0, TARGET_HEIGHT - 1],
    ])

    # Compute perspective transformation matrix
    perspective_transform = cv2.getPerspectiveTransform(SOURCE.astype(np.float32), TARGET.astype(np.float32))

    box_annotator = sv.BoxAnnotator(
        thickness=4,
        text_thickness=4,
        text_scale=2
    )
    video_info = sv.VideoInfo.from_video_path(SOURCE_VIDEO_PATH)

    frame_width = video_info.width
    left_lane_end = frame_width // 3  # Left lane boundary
    right_lane_start = 2 * (frame_width // 3)  # Right lane boundary

    # Initialize sequential ID mapping
    id_counter = 1
    id_map = {}  # Maps tracker_id to a sequential ID
    frame_data_list = []  # To store frame data
    vehicle_positions = {}
    vehicle_times = {}

    # Create directory for saving frames
    os.makedirs(FRAME_SAVE_DIR, exist_ok=True)

    # Define the crossing line position (horizontal line in the middle of the frame)
    line_y_position = video_info.height // 2  # Horizontal line in the middle of the frame

    # Variables to track vehicle crossings
    vehicle_crossings = {'entered': 0, 'exited': 0}
    crossing_tracker = {}  # Tracks if a vehicle has crossed the line

    id_counter = 1
    id_map = {} 


    generator = sv.video.get_video_frames_generator(SOURCE_VIDEO_PATH)

    # Extract the video name (without extension) for the JSON filename
    video_name = os.path.splitext(os.path.basename(SOURCE_VIDEO_PATH))[0]
    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    json_output_path = os.path.join(RESULTS_FOLDER, f"{video_name}_{now}.json")

    # Open output video stream
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
            # Extract frame and detections
            frame = result.orig_img
            detections = sv.Detections.from_yolov8(result)
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
                recording_time = datetime.now(timezone.utc)

            if recording_time:
                VIDEO_START_TIME = recording_time
            else:
                VIDEO_START_TIME = datetime.now()

            # Handle object IDs (tracker IDs)
            if result.boxes.id is not None:
        
                updated_tracker_ids, id_map, id_counter = handle_tracker_ids(result.boxes.id.cpu().numpy().astype(int), id_map, id_counter)
                detections.tracker_id = updated_tracker_ids
            
            # Check for vehicles crossing the line
            for bbox, confidence, class_id, tracker_id in detections:
                if tracker_id is None:
                    continue
                tracker_id = int(tracker_id)
                bbox = [float(coord) for coord in bbox]
                center_x = (bbox[0] + bbox[2]) / 2 # Center X of the bounding box
                center_y = (bbox[1] + bbox[3]) / 2  # Center Y of the bounding box

                
                if tracker_id not in crossing_tracker:
                    crossing_tracker[tracker_id] = {'crossed': False, 'last_position': bbox[1]}
                vehicle_color = get_exact_vehicle_color(bbox, frame)

                direction = get_vehicle_direction(center_x, center_y, tracker_id, vehicle_positions)
                lane = get_lane(center_x, left_lane_end, right_lane_start)

                # Update vehicle position
                vehicle_positions[tracker_id] = (center_x, center_y)
                
                # If vehicle is detected for the first time, initialize it in the tracker
                if tracker_id not in crossing_tracker:
                    crossing_tracker[tracker_id] = {'crossed': False, 'last_position': center_y}

                # Check if vehicle is crossing the line
                if not crossing_tracker[tracker_id]['crossed']:
                    # Vehicle is crossing the line from below
                    if center_y > line_y_position and crossing_tracker[tracker_id]['last_position'] <= line_y_position:
                        vehicle_crossings['entered'] += 1
                        crossing_tracker[tracker_id]['crossed'] = True  # Mark as crossed
                    # Vehicle is crossing the line from above
                    elif center_y < line_y_position and crossing_tracker[tracker_id]['last_position'] >= line_y_position:
                        vehicle_crossings['exited'] += 1
                        crossing_tracker[tracker_id]['crossed'] = True  # Mark as crossed

                # Update the vehicle's last position
                crossing_tracker[tracker_id]['last_position'] = center_y

                # Draw the crossing line on the frame
                cv2.line(frame, (0, line_y_position), (video_info.width, line_y_position), (0, 255, 0), 2)

                # Add entry and exit counts to the frame
                cv2.putText(frame, f"Entered: {vehicle_crossings['entered']}", (30, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 0, 0), 2)
                cv2.putText(frame, f"Exited: {vehicle_crossings['exited']}", (30, 100), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
        
                congestion_level = calculate_congestion_level(detections)
                speed = calculate_speed(center_x, center_y, tracker_id, frame_number)
                stopped = bool(is_stopped(speed))
        
                # Only draw if the vehicle is inside the polygon
                if is_in_target_polygon(center_x, center_y, SOURCE):
                    if tracker_id not in vehicle_times:
                        vehicle_times[tracker_id] = {"entry": frame_number, "exit": frame_number}
                    else:
                        vehicle_times[tracker_id]["exit"] = frame_number
                    label = f"ID: {tracker_id} | {model.model.names[class_id]} {confidence:0.2f}| Speed: {speed:.2f} km/h"
                    frame = box_annotator.annotate(
                                scene=frame,
                                detections=sv.Detections(
                                        xyxy=np.array([bbox]),  # Convert to numpy array
                                        confidence=np.array([confidence]),  # Convert to numpy array
                                        class_id=np.array([class_id]),  # Convert to numpy array
                                        tracker_id=np.array([tracker_id]) if tracker_id is not None else None  # Tracker ID optional
                                ),
                    labels=[label]
                    )
                    
        
                # Collect frame data for JSON with traffic congestion level and vehicle color
                    frame_data = {
                                "frame_number": frame_number,
                                "congestion_level": congestion_level,  # Add congestion level
                                "detections": [
                                    {
                                        "tracker_id": int(tracker_id),  # Convert to Python int
                                        "class_id": int(class_id),      # Convert to Python int
                                        "class_name": model.names[class_id],
                                        "direction": direction,
                                        "lane": lane,
                                        "vehicle_color": vehicle_color,
                                        "stopped": stopped,
                                        "speed" : speed,
                                        "confidence": float(confidence),  # Convert to Python float
                                        "bbox": [float(coord) for coord in bbox],  # Convert bbox to list of floats
                                        "entry_time": (
                        (VIDEO_START_TIME + timedelta(seconds=vehicle_times.get(int(tracker_id), {}).get("entry", 0) / FPS))
                        .strftime("%Y-%m-%d %H:%M:%S")
                        if vehicle_times.get(int(tracker_id)) else None
                    ),
                    "exit_time": (
                        (VIDEO_START_TIME + timedelta(seconds=vehicle_times.get(int(tracker_id), {}).get("exit", 0) / FPS))
                        .strftime("%Y-%m-%d %H:%M:%S")
                        if vehicle_times.get(int(tracker_id)) else None
                    )
                                    }
                                    for bbox, confidence, class_id, tracker_id in detections
                                ]
                            }
                    frame_data_list.append(frame_data)
                


                
            # Draw the source polygon
            cv2.polylines(frame, [SOURCE.astype(np.int32)], isClosed=True, color=(0, 255, 0), thickness=2)

            # Apply perspective transformation
            warped_frame = cv2.warpPerspective(frame, perspective_transform, (TARGET_WIDTH, TARGET_HEIGHT))
            # cv2.imwrite(f"warped_frame_{frame_number:04d}.jpg", warped_frame)

            # Save current frame to disk
            frame_path = os.path.join(FRAME_SAVE_DIR, f"frame_{frame_number:04d}.jpg")
            cv2.imwrite(frame_path, cv2.cvtColor(frame, cv2.COLOR_RGB2BGR))
                

            # Write annotated frame to the output video
            sink.write_frame(frame)

    # Save frame data to a JSON file
    with open(json_output_path, 'w') as json_file:
        json.dump(frame_data_list, json_file, indent=4)

    return json_output_path


@app.route("/upload_vehicle", methods=["POST"])
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
    app.run(host="0.0.0.0", port=8012, debug=True,use_reloader=False)