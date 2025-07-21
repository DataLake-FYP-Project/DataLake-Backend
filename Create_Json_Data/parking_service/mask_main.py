from flask import Flask, request, jsonify
import os
import cv2
import numpy as np
import pickle
import json
from datetime import datetime
import requests
from skimage.transform import resize
import warnings

# Suppress sklearn version warnings
warnings.filterwarnings("ignore", category=UserWarning)

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
RESULT_FOLDER = "results"
FRAME_SAVE_DIR = os.path.join(RESULT_FOLDER, "Frames")

# Create all required directories
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULT_FOLDER, exist_ok=True)
os.makedirs(FRAME_SAVE_DIR, exist_ok=True)

MASK_PATH = os.path.join(UPLOAD_FOLDER, "mask_crop.png")
MODEL_DIR = "Model"
MODEL_PATH = os.path.join(MODEL_DIR, "model.p")

SECOND_BACKEND_URL = "http://localhost:8013/upload_2_parking"

# Load the trained model with proper error handling
MODEL = None
try:
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(f"Model file not found at {MODEL_PATH}")
    
    os.makedirs(MODEL_DIR, exist_ok=True)
    
    with open(MODEL_PATH, "rb") as f:
        MODEL = pickle.load(f)
    print(f"Model loaded successfully from {MODEL_PATH}")
except Exception as e:
    print(f"Error loading model: {str(e)}")
    MODEL = None

EMPTY = True
NOT_EMPTY = False

def empty_or_not(spot_bgr):
    """Determine if a parking spot is empty using the trained model"""
    if MODEL is None:
        return NOT_EMPTY  # Default to occupied if model not loaded
    
    try:
        flat_data = []
        img_resized = resize(spot_bgr, (15, 15, 3))
        flat_data.append(img_resized.flatten())
        flat_data = np.array(flat_data)
        
        y_output = MODEL.predict(flat_data)
        return EMPTY if y_output == 0 else NOT_EMPTY
    except Exception as e:
        print(f"Error in empty_or_not: {str(e)}")
        return NOT_EMPTY

def get_parking_spots_bboxes(connected_components):
    """Extract bounding boxes from connected components"""
    (totalLabels, label_ids, values, centroid) = connected_components
    slots = []
    coef = 1
    for i in range(1, totalLabels):
        x1 = int(values[i, cv2.CC_STAT_LEFT] * coef)
        y1 = int(values[i, cv2.CC_STAT_TOP] * coef)
        w = int(values[i, cv2.CC_STAT_WIDTH] * coef)
        h = int(values[i, cv2.CC_STAT_HEIGHT] * coef)
        slots.append([x1, y1, w, h])
    return slots

def print_progress(frame_id, total_frames, free_count, total_slots):
    """Print processing progress with frame counter and slot status"""
    progress = (frame_id + 1) / total_frames * 100
    print(
        f"\rProcessing: {frame_id + 1}/{total_frames} frames "
        f"({progress:.1f}%) | "
        f"Free slots: {free_count}/{total_slots}",
        end="", flush=True
    )
    if frame_id + 1 == total_frames:
        print()

@app.route('/upload_mask_parking', methods=['POST'])
def process_parking():
    if 'file' not in request.files:
        return jsonify({"error": "Missing video file"}), 400

    video_file = request.files['file']
    video_path = os.path.join(UPLOAD_FOLDER, video_file.filename)
    video_file.save(video_path)

    # Load parking mask with error handling
    if not os.path.exists(MASK_PATH):
        return jsonify({"error": "Parking mask file not found."}), 400
        
    mask = cv2.imread(MASK_PATH, 0)
    if mask is None:
        return jsonify({"error": "Could not read parking mask."}), 400

    # Get parking slots using connected components
    connected_components = cv2.connectedComponentsWithStats(mask, 4, cv2.CV_32S)
    slots = get_parking_spots_bboxes(connected_components)

    if not slots:
        return jsonify({"error": "No parking slots detected in mask."}), 400

    # Initialize slot status with default values
    slot_status = {str(i+1): [{"frame": 0, "time": 0.0, "occupied": False}] for i in range(len(slots))}
    current_status = {str(i+1): False for i in range(len(slots))}
    occupancy_start_time = {}
    vehicle_data = {}

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return jsonify({"error": "Cannot open video."}), 401

    fps = cap.get(cv2.CAP_PROP_FPS)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    print(f"\nStarting video processing: {video_file.filename}")
    print(f"Total frames: {total_frames} | FPS: {fps:.2f}")
    print(f"Parking slots detected: {len(slots)}")
    print(f"Saving individual frames to: {FRAME_SAVE_DIR}")

    # Setup output paths
    SOURCE_VIDEO=video_file.filename
    video_name=os.path.splitext(SOURCE_VIDEO)[0]
    output_video_path = os.path.join(RESULT_FOLDER, f"annotated_{SOURCE_VIDEO}")
    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_json_path = os.path.join(RESULT_FOLDER, f"{video_name}_{now}.json")

    out = cv2.VideoWriter(output_video_path, cv2.VideoWriter_fourcc(*'mp4v'), fps, (width, height))

    frame_id = 0
    step = 30  # Process every 30 frames
    
    frame_data = [] 
    last_known_status = current_status.copy()

    while True:
        success, frame = cap.read()
        if not success:
            break

        frame_time = frame_id / fps
        free_count = 0
        current_frame_data = {
            "frame_number": frame_id,
            "timestamp_sec": round(frame_time, 2),
            "slots": {},
            "free_slots": 0
        }

        if frame_id % step == 0 or frame_id == 0:
            for idx, (x, y, w, h) in enumerate(slots):
                slot_id = str(idx + 1)
                spot_crop = frame[y:y + h, x:x + w, :]

                try:
                    spot_status = empty_or_not(spot_crop)
                    occupied = not spot_status

                    # Update last known status
                    last_known_status[slot_id] = occupied

                    # Append to slot_status
                    slot_status[slot_id].append({
                        "frame": frame_id,
                        "time": round(frame_time, 2),
                        "occupied": occupied
                    })

                    # Detect entry/exit
                    if current_status[slot_id] != occupied:
                        if occupied:
                            occupancy_start_time[slot_id] = frame_time
                        else:
                            if slot_id in occupancy_start_time:
                                duration = round(frame_time - occupancy_start_time[slot_id], 2)
                                vehicle_data.setdefault(slot_id, []).append({
                                    "entry_time": round(occupancy_start_time[slot_id], 2),
                                    "exit_time": round(frame_time, 2),
                                    "duration_sec": duration,
                                    "vehicle_type": "unknown"
                                })
                                del occupancy_start_time[slot_id]
                        current_status[slot_id] = occupied
                except Exception as e:
                    print(f"Error processing slot {slot_id}: {str(e)}")

        # Use last known state for this frame
        for idx, (x, y, w, h) in enumerate(slots):
            slot_id = str(idx + 1)
            occupied = last_known_status.get(slot_id, False)

            current_frame_data["slots"][slot_id] = {
                "occupied": occupied,
                "bbox": [x, y, w, h]
            }

            if not occupied:
                free_count += 1

            # Draw
            color = (0, 0, 255) if occupied else (0, 255, 0)
            cv2.rectangle(frame, (x, y), (x + w, y + h), color, 2)
            cv2.putText(frame, slot_id, (x + 2, y + h - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 1)

        current_frame_data["free_slots"] = free_count
        frame_data.append(current_frame_data)

        frame_filename = os.path.join(FRAME_SAVE_DIR, f"frame_{frame_id:04d}.jpg")
        cv2.imwrite(frame_filename, frame)

        print_progress(frame_id, total_frames, free_count, len(slots))

        cv2.putText(frame, f"Free: {free_count}/{len(slots)}", (10, 30),
                    cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 0), 2)

        out.write(frame)
        frame_id += 1


    # Handle any vehicles still parked at the end
    end_time = total_frames / fps
    for slot_id, start_time in occupancy_start_time.items():
        duration = round(end_time - start_time, 2)
        vehicle_data.setdefault(slot_id, []).append({
            "entry_time": round(start_time, 2),
            "exit_time": round(end_time, 2),
            "duration_sec": duration,
            "vehicle_type": "unknown"
        })

    cap.release()
    out.release()

    # Prepare the final JSON output
    output_json = {
        "processing_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "video_source": video_file.filename,
        "video_info": {
            "width": width,
            "height": height,
            "fps": fps,
            "total_frames": total_frames
        },
        "parking_config": {
            "total_slots": len(slots),
            "slot_coordinates": {
                str(i+1): [
                    [slots[i][0], slots[i][1]],
                    [slots[i][0] + slots[i][2], slots[i][1]],
                    [slots[i][0] + slots[i][2], slots[i][1] + slots[i][3]],
                    [slots[i][0], slots[i][1] + slots[i][3]]
                ]
                for i in range(len(slots))
            },
            "detection_method": "mask_based"
        },
        # "results": {
        #     "total_vehicles": sum(len(v) for v in vehicle_data.values()),
        #     "final_occupancy": current_status,
        #     "vehicle_data": vehicle_data,
        #     "occupancy_history": {k: v[1:] for k, v in slot_status.items()}  # Skip initial dummy entry
        # },
        "frames_directory": FRAME_SAVE_DIR,
        "frame_detections": frame_data
    }

    with open(output_json_path, "w") as jf:
        json.dump(output_json, jf, indent=4)

    print(f"\nProcessing complete! Results saved to:")
    print(f"- Annotated video: {output_video_path}")
    print(f"- JSON report: {output_json_path}")
    print(f"- Individual frames: {FRAME_SAVE_DIR} ({frame_id} frames saved)")

    try:
        with open(output_json_path, 'rb') as json_file:
            response = requests.post(SECOND_BACKEND_URL, files={"json_file": json_file}, timeout=10)
            response_data = response.json()

    except Exception as e:
        return jsonify({"error": f"Error during second backend communication: {str(e)}"}), 500
    
    return jsonify({
        "message": "Processed successfully",
        "annotated_video": output_video_path,
        "json_output": output_json_path,
        "frames_directory": FRAME_SAVE_DIR,
        "total_frames_saved": frame_id,
        "slots_detected": len(slots),
        "detection_method": "mask_based",
        "model_used": MODEL is not None
    }), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8018, debug=True, use_reloader=False)