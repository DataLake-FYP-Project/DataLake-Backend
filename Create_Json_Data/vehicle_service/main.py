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
import pandas as pd

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


# Red light violation detection
def detect_crossing_box(frame, red_light_points):
    return red_light_points


def detect_light_color(frame, box):
    x1, y1, x2, y2 = map(int, box)
    roi = frame[y1:y2, x1:x2]
    if roi.size == 0: return "unknown"
    hsv = cv2.cvtColor(roi, cv2.COLOR_BGR2HSV)
    r1 = cv2.inRange(hsv, (0, 70, 50), (10, 255, 255))
    r2 = cv2.inRange(hsv, (160, 70, 50), (180, 255, 255))
    g = cv2.inRange(hsv, (40, 40, 40), (80, 255, 255))
    y = cv2.inRange(hsv, (20, 100, 100), (30, 255, 255))
    rc, gc, yc = cv2.countNonZero(r1 | r2), cv2.countNonZero(g), cv2.countNonZero(y)
    if rc > gc and rc > yc and rc > 50: return "red"
    if gc > rc and gc > yc and gc > 50: return "green"
    if yc > rc and yc > gc and yc > 50: return "yellow"
    return "unknown"


# Define approximate_geolocation function first
def approximate_geolocation(bbox, camera_gps, heading, image_width, hfv=60.0):
    x_center = (bbox[0] + bbox[2]) / 2
    bbox_height = bbox[3] - bbox[1]
    k = 5000  # Can be adjusted based on setup
    distance = min(k / bbox_height, 50)
    image_center_x = image_width / 2
    azimuth = ((x_center - image_center_x) / image_center_x) * (hfv / 2)
    true_azimuth = azimuth + heading
    azimuth_rad = np.deg2rad(true_azimuth)
    lat_per_meter = 1 / 111139
    lon_per_meter = 1 / (111139 * np.cos(np.deg2rad(camera_gps['latitude'])))
    lat_offset = distance * np.cos(azimuth_rad) * lat_per_meter
    lon_offset = distance * np.sin(azimuth_rad) * lon_per_meter
    return camera_gps['latitude'] + lat_offset, camera_gps['longitude'] + lon_offset


# Run custom model for geolocation estimation
def CustomModelRun(SOURCE_VIDEO_PATH, TARGET_VIDEO_PATH, camera_metadata):
    custom_model_path = os.path.join("Model", "yolov8_custom.pt")
    model = YOLO(custom_model_path)
    model.fuse()

    # Define output paths
    output_video_path = TARGET_VIDEO_PATH
    output_geolocation_dir = os.path.join(os.path.dirname(TARGET_VIDEO_PATH), "detections_with_gps_video")
    os.makedirs(output_geolocation_dir, exist_ok=True)

    # Open the video
    cap = cv2.VideoCapture(SOURCE_VIDEO_PATH)
    if not cap.isOpened():
        print("Error: Could not open video.")
        return

    # Get video properties
    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = int(cap.get(cv2.CAP_PROP_FPS))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    # Define the codec and create VideoWriter object
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(output_video_path, fourcc, fps, (frame_width, frame_height))

    # Process each frame
    frame_number = 0
    all_detections = []

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        # Run detection
        results = model(frame, conf=0.1, iou=0.5)
        detections = []

        # Use provided camera_metadata
        camera_gps = {'latitude': camera_metadata['latitude'], 'longitude': camera_metadata['longitude']}
        heading = camera_metadata['heading']

        # Process detections
        for result in results:
            boxes = result.boxes.xyxy.cpu().numpy()
            confidences = result.boxes.conf.cpu().numpy()
            classes = result.boxes.cls.cpu().numpy()
            for box, conf, cls in zip(boxes, confidences, classes):
                x1, y1, x2, y2 = map(int, box)
                class_name = result.names[int(cls)]
                color = (255, 0, 255) if class_name == 'tuk-tuk' else (0, 255, 0)

                # Draw bounding box and label
                cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
                cv2.putText(frame, f'{class_name} {conf:.2f}', (x1, y1 - 10),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)

                # Estimate geolocation
                lat, lon = approximate_geolocation([x1, y1, x2, y2], camera_gps, heading, frame_width)
                detections.append({
                    'frame': frame_number,
                    'class': class_name,
                    'confidence': float(conf),
                    'bbox': [float(x1), float(y1), float(x2), float(y2)],
                    'geolocation': {'latitude': lat, 'longitude': lon}
                })

                # Display geolocation on the frame
                cv2.putText(frame, f'Lat: {lat:.4f}, Lon: {lon:.4f}', (x1, y2 + 20),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)

        # Write the frame to the output video
        out.write(frame)

        # Store detections
        all_detections.extend(detections)
        print(f'Processed frame {frame_number}/{total_frames}')
        frame_number += 1

    # Release resources
    cap.release()
    out.release()
    cv2.destroyAllWindows()

    # Save all detections with geolocation
    video_name = os.path.splitext(os.path.basename(SOURCE_VIDEO_PATH))[0]
    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_json = os.path.join(output_geolocation_dir, f"{video_name}_{now}.json")
    with open(output_json, 'w') as f:
        json.dump(all_detections, f, indent=4)

    print(f'Annotated video saved to {output_video_path}')
    print(f'Geolocation data saved to {output_json}')

    return output_json


# Create metadata.csv
def create_metadata_csv(video_path, camera_metadata, output_path):
    cap = cv2.VideoCapture(video_path)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    cap.release()

    metadata_list = []
    for frame_number in range(total_frames):
        metadata_list.append({
            'frame_number': frame_number,
            'latitude': camera_metadata['latitude'],
            'longitude': camera_metadata['longitude'],
            'heading': camera_metadata['heading']
        })

    metadata_df = pd.DataFrame(metadata_list)
    metadata_df.to_csv(output_path, index=False)
    return output_path


def ModelRun(SOURCE_VIDEO_PATH, TARGET_VIDEO_PATH, ex_points, red_light_points, line_points):
    model_path = os.path.join("Model", "yolov8x.pt")
    model = YOLO(model_path)
    model.fuse()

    SOURCE = np.array(ex_points)
    TARGET_WIDTH = 25
    TARGET_HEIGHT = 250

    LINE_POINTS = np.array(line_points)
    x1, y1 = LINE_POINTS[0]
    x2, y2 = LINE_POINTS[1]
    line = LineString([(x1, y1), (x2, y2)])

    TARGET = np.array([
        [0, 0],
        [TARGET_WIDTH - 1, 0],
        [TARGET_WIDTH - 1, TARGET_HEIGHT - 1],
        [0, TARGET_HEIGHT - 1],
    ])

    # Compute perspective transformation matrix
    perspective_transform = cv2.getPerspectiveTransform(SOURCE.astype(np.float32), TARGET.astype(np.float32))

    crossing_box = None
    if red_light_points:
        cap_tmp = cv2.VideoCapture(SOURCE_VIDEO_PATH)
        _, first = cap_tmp.read()
        cap_tmp.release()

        # Just return the 4 points
        crossing_box = detect_crossing_box(first, red_light_points)

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
    red_violators = set()
    id_map, id_ctr = {}, 1
    violation_times = {}
    line_violation_times = {}

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
            frame = result.orig_img
            detections = sv.Detections.from_yolov8(result)
            video_metadata = extract_video_metadata(SOURCE_VIDEO_PATH)

            # Get recording time from metadata or use current time as fallback
            if "creation_time" in video_metadata and video_metadata["creation_time"]:
                try:
                    # Attempt to split and convert creation_time to a datetime object
                    recording_time = datetime.strptime(video_metadata["creation_time"].split(".")[0],
                                                       "%Y-%m-%dT%H:%M:%S")
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

            if result.boxes.id is not None:

                updated_tracker_ids, id_map, id_counter = handle_tracker_ids(result.boxes.id.cpu().numpy().astype(int),
                                                                             id_map, id_counter)
                detections.tracker_id = updated_tracker_ids

                # Traffic light detection
                light = "unknown"
                if result.boxes.id is not None:
                    for box, cls in zip(result.boxes.xyxy, result.boxes.cls):
                        if model.model.names[int(cls)] == 'traffic light':
                            light = detect_light_color(frame, box)
                            cv2.rectangle(frame, tuple(map(int, box[:2])), tuple(map(int, box[2:])),
                                          (0, 0, 255) if light == 'red' else (0, 255, 0), 2)
                            break

                # Remap tracker IDs
                if result.boxes.id is not None:
                    tracker_ids = result.boxes.id.cpu().numpy().astype(int);
                    upd = []
                    for t in tracker_ids:
                        if t not in id_map: id_map[t] = id_ctr; id_ctr += 1
                        upd.append(id_map[t])
                    detections.tracker_id = upd

            frame_detections = {}
            for bbox, confidence, class_id, tracker_id in detections:
                if tracker_id is None:
                    continue
                tracker_id = int(tracker_id)
                bbox = [float(coord) for coord in bbox]
                center_x = (bbox[0] + bbox[2]) / 2
                center_y = (bbox[1] + bbox[3]) / 2

                if tracker_id not in crossing_tracker:
                    crossing_tracker[tracker_id] = {'crossed': False, 'last_position': bbox[1]}
                vehicle_color = get_exact_vehicle_color(bbox, frame)

                direction = get_vehicle_direction(center_x, center_y, tracker_id, vehicle_positions)
                lane = get_lane(center_x, left_lane_end, right_lane_start)

                vehicle_positions[tracker_id] = (center_x, center_y)

                # 🚥 Original line crossing logic
                if not crossing_tracker[tracker_id]['crossed']:
                    if center_y > line_y_position and crossing_tracker[tracker_id]['last_position'] <= line_y_position:
                        vehicle_crossings['entered'] += 1
                        crossing_tracker[tracker_id]['crossed'] = True  # Mark as crossed
                    elif center_y < line_y_position and crossing_tracker[tracker_id][
                        'last_position'] >= line_y_position:
                        vehicle_crossings['exited'] += 1
                        crossing_tracker[tracker_id]['crossed'] = True  # Mark as crossed

                tracker_id_int = int(tracker_id)
                crossing_tracker[tracker_id]['last_position'] = center_y

                cv2.line(frame, (0, line_y_position), (video_info.width, line_y_position), (0, 255, 0), 2)
                cv2.line(frame, (x1, y1), (x2, y2), (0, 0, 255), 4)
                cv2.putText(frame, f"Entered: {vehicle_crossings['entered']}", (30, 50), cv2.FONT_HERSHEY_SIMPLEX, 1,
                            (255, 0, 0), 2)
                cv2.putText(frame, f"Exited: {vehicle_crossings['exited']}", (30, 100), cv2.FONT_HERSHEY_SIMPLEX, 1,
                            (0, 0, 255), 2)

                congestion_level = calculate_congestion_level(detections)
                speed = calculate_speed(center_x, center_y, tracker_id, frame_number)
                stopped = bool(is_stopped(speed))

                # --- NEW VIOLATION: ENTERING the blue box while red ---
                violation = False
                red_violation_time = None
                if crossing_box is not None:
                    inside = (crossing_box[0][0] <= center_x <= crossing_box[1][0]
                              and crossing_box[0][1] <= center_y <= crossing_box[1][1])

                    if inside and light == 'red' and tracker_id not in red_violators:
                        violation = True
                        red_violators.add(tracker_id)
                        if tracker_id_int not in violation_times:
                            violation_times[tracker_id_int] = frame_number

                    crossing_tracker[tracker_id]['inside'] = inside

                    red_violation_frame = violation_times.get(tracker_id_int)
                    if violation and red_violation_frame is not None:
                        red_violation_time = (
                                VIDEO_START_TIME + timedelta(seconds=red_violation_frame / FPS)
                        ).strftime("%Y-%m-%d %H:%M:%S")

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
                    line_crossing = False
                    line_violation_time = None

                    center_point = Point(center_x, center_y)
                    bbox_width = bbox[2] - bbox[0]
                    distance = center_point.distance(line)
                    line_crossing = distance <= (bbox_width / 2)

                    # Store frame number for first violation
                    if line_crossing and tracker_id not in line_violation_times:
                        line_violation_times[tracker_id] = frame_number

                    # Calculate line crossing violation time
                    violation_frame = line_violation_times.get(tracker_id)
                    if line_crossing and violation_frame is not None:
                        line_violation_time = (
                                VIDEO_START_TIME + timedelta(seconds=violation_frame / FPS)
                        ).strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        line_violation_time = None

                    if tracker_id not in frame_detections:
                        frame_detections[tracker_id] = {
                            "tracker_id": int(tracker_id),
                            "confidence": float(confidence),
                            "bbox": [float(coord) for coord in bbox],
                            "class_id": int(class_id),
                            "vehicle_type": model.names[class_id],
                            "vehicle_direction": direction,
                            "vehicle_lane": lane,
                            "vehicle_color": vehicle_color,
                            "stopped": stopped,
                            "vehicle_speed": speed,
                            "red_light_violation": violation,
                            "red_light_violation_time": red_violation_time,
                            "line_crossing": line_crossing,
                            "line_crossing_violation_time": line_violation_time,
                            "vehicle_entry_time": (
                                (VIDEO_START_TIME + timedelta(
                                    seconds=vehicle_times.get(int(tracker_id), {}).get("entry", 0) / FPS))
                                .strftime("%Y-%m-%d %H:%M:%S")
                                if vehicle_times.get(int(tracker_id)) else None
                            ),
                            "vehicle_exit_time": (
                                (VIDEO_START_TIME + timedelta(
                                    seconds=vehicle_times.get(int(tracker_id), {}).get("exit", 0) / FPS))
                                .strftime("%Y-%m-%d %H:%M:%S")
                                if vehicle_times.get(int(tracker_id)) else None
                            )
                        }
                    else:
                        # Update the existing entry if needed (e.g., in case of multiple detections per tracker)
                        frame_detections[tracker_id].update({
                            "confidence": max(frame_detections[tracker_id]["confidence"], confidence),
                            "vehicle_speed": max(frame_detections[tracker_id]["vehicle_speed"], speed),
                            # Add other updates as needed
                        })

            frame_data = {
                "frame_number": frame_number,
                "congestion_level": congestion_level,
                "traffic_light": light,
                "detections": list(frame_detections.values())
            }
            frame_data_list.append(frame_data)

            cv2.polylines(frame, [SOURCE.astype(np.int32)], isClosed=True, color=(0, 255, 0), thickness=2)
            warped_frame = cv2.warpPerspective(frame, perspective_transform, (TARGET_WIDTH, TARGET_HEIGHT))
            cv2.putText(frame, f"Light:{light.upper()}", (30, 150),
                        cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 255), 2)
            if crossing_box is not None:
                pts = np.array(crossing_box, np.int32).reshape((-1, 1, 2))
                cv2.polylines(frame, [pts], isClosed=True, color=(255, 0, 0), thickness=2)
            # cv2.imwrite(f"warped_frame_{frame_number:04d}.jpg", warped_frame)
            frame_path = os.path.join(FRAME_SAVE_DIR, f"frame_{frame_number:04d}.jpg")
            cv2.imwrite(frame_path, cv2.cvtColor(frame, cv2.COLOR_RGB2BGR))
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
    print("points", points)

    # Extract metadata from the request
    metadata = request.form.get("metadata", None)
    camera_metadata = None
    if metadata:
        try:
            camera_metadata = json.loads(metadata)
            print("Received camera metadata:", camera_metadata)
        except json.JSONDecodeError as e:
            print(f"Error parsing metadata JSON: {e}")
            return jsonify({"error": f"Invalid JSON in metadata: {e}"}), 400

    elif points:
        try:
            points = json.loads(points)
            # Separate the 3 point types
            ex_points = points.get("Area", [])
            red_light_points = points.get("red_light", [])
            line_points = points.get("line_points", [])
        except json.JSONDecodeError as e:
            return jsonify({"error": f"Invalid JSON in points: {e}"}), 400
    else:
        ex_points = []
        red_light_points = []
        line_points = []

    source_video_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(source_video_path)

    target_video_path = os.path.join(RESULTS_FOLDER, "processed_" + file.filename)

    try:
        print(f"SOURCE_VIDEO_PATH: {source_video_path}")
        print(f"TARGET_VIDEO_PATH: {target_video_path}")
        print(f"FRAME_SAVE_DIR: {FRAME_SAVE_DIR}")

        if camera_metadata:
            json_output_path = CustomModelRun(
                source_video_path, target_video_path,camera_metadata
            )
        else:
            json_output_path = ModelRun(
                source_video_path, target_video_path,
                ex_points, red_light_points, line_points
            )
        print(f"JSON Output Path: {json_output_path}")

        # Create metadata.csv after ModelRun
        metadata_csv_path = None
        if camera_metadata:
            # Ensure all required metadata fields are present
            required_fields = ["latitude", "longitude", "heading"]
            if all(field in camera_metadata and camera_metadata[field] is not None for field in required_fields):
                metadata_csv_path = os.path.join(RESULTS_FOLDER, f"metadata_{file.filename}.csv")
                create_metadata_csv(source_video_path, camera_metadata, metadata_csv_path)
                print(f"Metadata CSV created at: {metadata_csv_path}")
            else:
                print("Skipping metadata CSV creation: Missing or invalid metadata fields")

    except Exception as e:
        print(f"Error in ModelRun: {str(e)}")
        return jsonify({"error": f"Error processing video: {str(e)}"}), 500

    try:
        with open(json_output_path, 'rb') as json_file:
            # Optionally include metadata_csv_path in the second backend request
            files = {"json_file": json_file}
            if metadata_csv_path and os.path.exists(metadata_csv_path):
                files["metadata_csv"] = open(metadata_csv_path, 'rb')
            response = requests.post(SECOND_BACKEND_URL, files=files, timeout=10)
            response_data = response.json()

    except Exception as e:
        return jsonify({"error": f"Error during second backend communication: {str(e)}"}), 500
    finally:
        # Clean up file handles if opened
        if "metadata_csv" in files and files["metadata_csv"]:
            files["metadata_csv"].close()

    return jsonify({
        "message": "File uploaded and processed successfully",
        "source_video": source_video_path,
        "processed_video": target_video_path,
        "json_output": json_output_path,
        "metadata_csv": metadata_csv_path if metadata_csv_path else "Not created",
        "second_backend_response": response_data
    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8012, debug=True, use_reloader=False)
