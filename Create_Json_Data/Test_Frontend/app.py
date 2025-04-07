import json
import streamlit as st
import requests
import cv2
import tempfile


# Function to capture points on the video
def select_points_on_video(video_path):
    # Initialize video capture
    cap = cv2.VideoCapture(video_path)
    ret, frame = cap.read()

    # List to store clicked points
    points = []

    # Mouse callback function to store points
    def click_event(event, x, y, flags, param):
        if event == cv2.EVENT_LBUTTONDOWN:
            if len(points) < 4:  # Limit to 4 points
                points.append((x, y))
                print(f"Point {len(points)}: ({x}, {y})")
            if len(points) == 4:
                cv2.destroyAllWindows()

    # Capture the first frame
    if ret:
        frame = cv2.resize(frame, (640, 480))

        # Show the first frame
        cv2.imshow("Click to Select Points", frame)
        cv2.setMouseCallback("Click to Select Points", click_event)

        # Wait until 4 points are clicked
        while len(points) < 4:
            cv2.waitKey(1)  # Allow OpenCV to process mouse events

        cv2.destroyAllWindows()

    cap.release()

    return points


def upload_video_and_points(video_file, points):
    url = "http://localhost:8011/upload"

    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
        tmp.write(video_file.read())
        tmp_path = tmp.name

    points_data = json.dumps(points)

    # Rewind the file pointer before upload
    video_file.seek(0)

    files = {"file": (video_file.name, video_file, "video/mp4")}
    data = {"points": points_data}  # if backend expects points
    response = requests.post(url, files=files, data=data)

    return response


# Streamlit UI to upload video and select points
st.title("Video Uploader with Points Selection")

video_file = st.file_uploader("Choose a video file", type=["mp4", "avi", "mov", "mkv"])

if video_file:
    st.video(video_file)

    # Save the video temporarily for processing using tempfile
    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as temp_video_file:
        temp_video_file.write(video_file.read())
        temp_video_path = temp_video_file.name
        st.write(f"Temporary video file saved at: {temp_video_path}")

    # Initialize session state for points
    if 'points' not in st.session_state:
        st.session_state.points = []

    # Button to start selecting points
    if st.button("Select Points on Video"):
        points = select_points_on_video(temp_video_path)

        # Store the selected points in session state
        st.session_state.points = points

        st.write(f"Selected points: {points}")

    # Button to upload video along with selected points
    if st.button("Upload Video"):
        points = st.session_state.points

        if points:
            with st.spinner("Uploading..."):
                response = upload_video_and_points(video_file, points)

            if response.status_code == 200:
                st.success("Video uploaded successfully!")
            else:
                st.error(f"Upload failed: {response.status_code} - {response.text}")
        else:
            st.warning("Please select 4 points first!")
