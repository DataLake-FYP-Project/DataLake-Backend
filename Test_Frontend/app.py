import json
import streamlit as st
import requests
import cv2
import tempfile
import os


# Function to capture points on the video
def select_points_on_video(video_path, new_width=640, new_height=480):
    cap = cv2.VideoCapture(video_path)
    ret, frame = cap.read()
    if not ret:
        st.error("Failed to read the video file.")
        return []

    original_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    original_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    frame_resized = cv2.resize(frame, (new_width, new_height))
    points = []

    def click_event(event, x, y, flags, param):
        if event == cv2.EVENT_LBUTTONDOWN and len(points) < 4:
            points.append((x, y))
            print(f"Point {len(points)}: ({x}, {y})")
            if len(points) == 4:
                cv2.destroyAllWindows()

    cv2.imshow("Click to Select Points", frame_resized)
    cv2.setMouseCallback("Click to Select Points", click_event)

    while len(points) < 4:
        cv2.waitKey(1)

    cv2.destroyAllWindows()
    cap.release()

    return scale_polygon_points(points, original_width, original_height, new_width, new_height)


def scale_polygon_points(polygon_points, original_width, original_height, new_width, new_height):
    scale_x = original_width / new_width
    scale_y = original_height / new_height
    return [(int(x * scale_x), int(y * scale_y)) for x, y in polygon_points]


def upload_video_and_points(video_file, points, video_type):
    url = {
        "People": "http://localhost:8011/upload_people",
        "Vehicle": "http://localhost:8012/upload_vehicle"
    }.get(video_type)

    if not url:
        st.error("Invalid video type selected.")
        return

    points_data = json.dumps(points)
    video_file.seek(0)

    files = {"file": (video_file.name, video_file, "video/mp4")}
    data = {"points": points_data}

    try:
        response = requests.post(url, files=files, data=data)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        st.error(f"Upload failed: {str(e)}")
        return


# Streamlit UI
st.title("Video Uploader with Points Selection")

video_file = st.file_uploader("Choose a video file", type=["mp4", "avi", "mov", "mkv"])

if video_file:
    st.video(video_file)

    if 'points' not in st.session_state:
        st.session_state.points = []

    video_type = st.selectbox("Select Video Type", ["People", "Vehicle"])

    if st.button("Select Points on Video"):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
            tmp.write(video_file.read())
            tmp_path = tmp.name

        points = select_points_on_video(tmp_path)
        os.remove(tmp_path)  # Remove after selection

        if points:
            st.session_state.points = points
            st.write(f"Selected points: {points}")
        else:
            st.error("No points selected.")

    if st.button("Upload Video"):
        points = st.session_state.points
        if points:
            with st.spinner("Uploading..."):
                response = upload_video_and_points(video_file, points, video_type)

            if response:
                if response.status_code == 200:
                    st.success("Video uploaded successfully!")
                else:
                    st.error(f"Upload failed: {response.status_code} - {response.text}")
        else:
            st.warning("Please select points before uploading.")
