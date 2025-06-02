import os
import requests
from flask import Flask, request, jsonify
import cv2
import mediapipe as mp
import json
import numpy as np
import pandas as pd
import pickle

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
RESULTS_FOLDER = "results"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)
FRAME_SAVE_DIR = 'results/frames/'
os.makedirs(FRAME_SAVE_DIR, exist_ok=True)

SECOND_BACKEND_URL = "http://localhost:8013/upload_2_pose"

# Initialize MediaPipe Holistic
mp_holistic = mp.solutions.holistic
holistic = mp_holistic.Holistic(min_detection_confidence=0.5, min_tracking_confidence=0.5)
mp_drawing = mp.solutions.drawing_utils

# Load the pre-trained action recognition model
MODEL_PATH = os.path.join("Model", "saved_model.pkl")
with open(MODEL_PATH, 'rb') as f:
    action_model = pickle.load(f)


def ModelRun(SOURCE_VIDEO_PATH, TARGET_VIDEO_PATH):
    """
    Process a video for pose estimation and action recognition.

    Args:
        SOURCE_VIDEO_PATH (str): Path to the input video.
        TARGET_VIDEO_PATH (str): Path to save the annotated output video.

    Returns:
        str: Path to the JSON file containing pose and action data.
    """
    frame_data_list = []

    # Get video info
    cap = cv2.VideoCapture(SOURCE_VIDEO_PATH)
    if not cap.isOpened():
        raise ValueError("Error opening video file")

    # Video properties
    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = int(cap.get(cv2.CAP_PROP_FPS))

    # Initialize video writer
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(TARGET_VIDEO_PATH, fourcc, fps, (frame_width, frame_height))

    # Prepare JSON output path
    video_name = os.path.splitext(os.path.basename(SOURCE_VIDEO_PATH))[0]
    json_output_path = os.path.join(RESULTS_FOLDER, f"{video_name}_pose_action.json")

    frame_number = 0
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        # Convert frame to RGB for MediaPipe
        image = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        image.flags.writeable = False
        results = holistic.process(image)
        image.flags.writeable = True
        image = cv2.cvtColor(image, cv2.COLOR_RGB2BGR)

        # Pose estimation and action recognition
        pose_info = []
        if results.pose_landmarks:
            # Draw pose landmarks
            mp_drawing.draw_landmarks(
                image,
                results.pose_landmarks,
                mp_holistic.POSE_CONNECTIONS,
                mp_drawing.DrawingSpec(color=(245, 117, 66), thickness=2, circle_radius=4),
                mp_drawing.DrawingSpec(color=(245, 66, 230), thickness=2, circle_radius=2)
            )

            # Extract pose keypoints
            pose = results.pose_landmarks.landmark
            pose_row = np.array(
                [[landmark.x, landmark.y, landmark.z, landmark.visibility] for landmark in pose]).flatten()

            # Predict action with correct feature names
            num_landmarks = 33  # Number of pose landmarks in MediaPipe
            feature_names = []
            for i in range(1, num_landmarks + 1):
                feature_names.extend([f'x{i}', f'y{i}', f'z{i}', f'v{i}'])
            X = pd.DataFrame([pose_row], columns=feature_names)
            action_class = action_model.predict(X)[0]
            action_prob = action_model.predict_proba(X)[0]
            max_prob = round(action_prob[np.argmax(action_prob)], 2)

            # Store pose data
            keypoints = [{"landmark_id": idx // 4, "x": float(x), "y": float(y), "z": float(z), "visibility": float(v)}
                         for idx, (x, y, z, v) in
                         enumerate(zip(pose_row[::4], pose_row[1::4], pose_row[2::4], pose_row[3::4]))]
            pose_info.append({
                "keypoints": keypoints,
                "action": action_class,
                "confidence": max_prob
            })

            # Annotate action on the frame
            label = f"Action: {action_class} ({max_prob:.2f})"
            cv2.putText(image, label, (50, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)

        # Save annotated frame
        frame_path = os.path.join(FRAME_SAVE_DIR, f"frame_{frame_number:04d}.jpg")
        cv2.imwrite(frame_path, image)

        # Save JSON info
        frame_data_list.append({
            "frame_number": frame_number,
            "pose_data": pose_info
        })

        # Write annotated frame to video
        out.write(image)
        frame_number += 1

    # Release resources
    cap.release()
    out.release()

    print("Pose estimation and action recognition completed.")

    # Save frame data to a JSON file
    with open(json_output_path, 'w') as json_file:
        json.dump(frame_data_list, json_file, indent=4)

    return json_output_path


@app.route("/upload_pose", methods=["POST"])
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
    app.run(host="0.0.0.0", port=8015, debug=True, use_reloader=False)
