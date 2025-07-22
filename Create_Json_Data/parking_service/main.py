from flask import Flask, request, jsonify
import os
import cv2
import numpy as np
import pickle
import json
from datetime import datetime

import requests

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
RESULT_FOLDER = "results"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULT_FOLDER, exist_ok=True)

FRAME_SAVE_DIR = os.path.join(RESULT_FOLDER, "Frames") 
os.makedirs(FRAME_SAVE_DIR, exist_ok=True)  

SLOTS_FILE = os.path.join(UPLOAD_FOLDER, "parking_slot_coords.pkl")

SECOND_BACKEND_URL = "http://localhost:8013/upload_2_parking"

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
        print()  # New line when complete

@app.route('/upload_parking', methods=['POST'])
def process_parking():
    if 'file' not in request.files:
        return jsonify({"error": "Missing video file"}), 400

    video_file = request.files['file']
    video_path = os.path.join(UPLOAD_FOLDER, video_file.filename)
    video_file.save(video_path)

    # Load parking slot coordinates
    if not os.path.exists(SLOTS_FILE):
        return jsonify({"error": "Parking slot coordinate file not found."}), 400

    with open(SLOTS_FILE, "rb") as f:
        slots = pickle.load(f)

    # Setup output paths
    SOURCE_VIDEO=video_file.filename
    video_name=os.path.splitext(SOURCE_VIDEO)[0]
    output_video_path = os.path.join(RESULT_FOLDER, f"annotated_{SOURCE_VIDEO}")
    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_json_path = os.path.join(RESULT_FOLDER, f"{video_name}_{now}.json")

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return jsonify({"error": "Cannot open video."}), 400

    fps = cap.get(cv2.CAP_PROP_FPS)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    print(f"\nStarting video processing: {video_file.filename}")
    print(f"Total frames: {total_frames} | FPS: {fps:.2f}")
    print(f"Parking slots configured: {len(slots)}")
    print(f"Saving individual frames to: {FRAME_SAVE_DIR}")

    out = cv2.VideoWriter(output_video_path, cv2.VideoWriter_fourcc(*'mp4v'), fps, (width, height))

    def preprocess_frame(frame):
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        blur = cv2.GaussianBlur(gray, (5, 5), 1)
        thresh = cv2.adaptiveThreshold(blur, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                      cv2.THRESH_BINARY_INV, 25, 16)
        dilated = cv2.dilate(thresh, np.ones((3, 3), np.uint8), iterations=1)
        return dilated

    frame_id = 0
    slot_status = {str(i+1): [] for i in range(len(slots))}
    occupancy_start_time = {}
    vehicle_data = {}
    current_occupancy = {str(i+1): False for i in range(len(slots))}
    THRESHOLD = 900

    frame_data = [] 
    while True:
        success, frame = cap.read()
        if not success:
            break

        frame_time_sec = frame_id / fps
        processed = preprocess_frame(frame)
        free_count = 0
        frame_entry = {
            "frame_number": frame_id,
            "timestamp_sec": round(frame_time_sec, 2),
            "slots": {},
            "free_slots": 0
        }

        for idx, (x, y, w, h) in enumerate(slots):
            slot_id = str(idx + 1)
            crop = processed[y:y+h, x:x+w]
            count = cv2.countNonZero(crop)
            occupied = count >= THRESHOLD
            color = (0, 0, 255) if occupied else (0, 255, 0)

            cv2.rectangle(frame, (x, y), (x + w, y + h), color, 2)
            cv2.putText(frame, slot_id, (x+2, y+h-5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 1)

            frame_entry["slots"][slot_id] = {
                "occupied": occupied,
                "bbox": [x, y, w, h],
                "pixel_count": int(count)  # Added pixel count
            }

            if current_occupancy[slot_id] != occupied:
                if occupied:
                    occupancy_start_time[slot_id] = frame_time_sec
                else:
                    if slot_id in occupancy_start_time:
                        duration = round(frame_time_sec - occupancy_start_time[slot_id], 2)
                        vehicle_data.setdefault(slot_id, []).append({
                            "entry_time": round(occupancy_start_time[slot_id], 2),
                            "exit_time": round(frame_time_sec, 2),
                            "duration_sec": duration,
                            "vehicle_type": "unknown"
                        })
                        del occupancy_start_time[slot_id]
                current_occupancy[slot_id] = occupied

            if not occupied:
                free_count += 1

        frame_entry["free_slots"] = free_count
        frame_data.append(frame_entry)

        # Save individual frame
        frame_filename = os.path.join(FRAME_SAVE_DIR, f"frame_{frame_id:04d}.jpg")
        cv2.imwrite(frame_filename, frame)

        # Display free spots count
        cv2.putText(frame, f"Free: {free_count}/{len(slots)}", (10, 30),
                   cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 0), 2)
        
        out.write(frame)
        print_progress(frame_id, total_frames, free_count, len(slots))
        frame_id += 1

    end_time = total_frames / fps
    for slot_id, start in occupancy_start_time.items():
        vehicle_data.setdefault(slot_id, []).append({
            "entry_time": round(start, 2),
            "exit_time": round(end_time, 2),
            "duration_sec": round(end_time - start, 2),
            "vehicle_type": "unknown"
        })

    cap.release()
    out.release()

    # In the output_json dictionary construction (around line 180), modify to:
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
            "detection_method": "manual"  # Added this line
        },
        # "results": {
        #     "total_vehicles": sum(len(v) for v in vehicle_data.values()),
        #     "final_occupancy": {k: v[-1]["occupied"] if v else False for k, v in slot_status.items()},
        #     "vehicle_data": vehicle_data,
        #     "occupancy_history": slot_status  # Added this line (uses existing slot_status data)
        # },
        "frames_directory": FRAME_SAVE_DIR,
        "frame_detections": frame_data
    }

    with open(output_json_path, "w") as jf:
        json.dump(output_json, jf, indent=4)

    print(f"\nProcessing complete! Results saved to:\n")
    print(f"TARGET_VIDEO_PATH: {output_video_path}")
    print(f"JSON_PATH: {output_json_path}")
    print(f"FRAME_SAVE_DIR: {FRAME_SAVE_DIR} ")

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
        "slots_file": SLOTS_FILE
    }), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8017, debug=True, use_reloader=False)