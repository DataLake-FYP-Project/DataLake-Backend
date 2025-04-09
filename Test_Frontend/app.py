import json
import streamlit as st
import requests
import cv2
import tempfile


# python -m streamlit run app.py

# Function to capture points on the video
def select_points_on_video(video_path, new_width=640, new_height=480):
    # Initialize video capture
    cap = cv2.VideoCapture(video_path)
    ret, frame = cap.read()

    # Get the original video dimensions
    original_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    original_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    # Resize the frame to the new size for selecting points
    frame_resized = cv2.resize(frame, (new_width, new_height))

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

    # Show the resized frame
    cv2.imshow("Click to Select Points", frame_resized)
    cv2.setMouseCallback("Click to Select Points", click_event)

    # Wait until 4 points are clicked
    while len(points) < 4:
        cv2.waitKey(1)  # Allow OpenCV to process mouse events

    cv2.destroyAllWindows()
    cap.release()

    # Scale the points to match the original frame dimensions
    scaled_points = scale_polygon_points(points, original_width, original_height, new_width, new_height)

    return scaled_points


def scale_polygon_points(polygon_points, original_width, original_height, new_width, new_height):
    scale_x = original_width / new_width
    scale_y = original_height / new_height
    return [(int(x * scale_x), int(y * scale_y)) for x, y in polygon_points]


def upload_video_and_points(video_file, points, video_type):
    if video_type == "People":
        url = "http://localhost:8011/upload"
    elif video_type == "Vehicle":
        url = "http://localhost:8012/upload"
    else:
        raise ValueError("Invalid video type selected")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
        tmp.write(video_file.read())
        tmp_path = tmp.name

    points_data = json.dumps(points)

    video_file.seek(0)  # Reset file pointer

    files = {"file": (video_file.name, video_file, "video/mp4")}
    data = {"points": points_data}

    try:
        response = requests.post(url, files=files, data=data)
        response.raise_for_status()  # Raise HTTPError for bad responses
        return response
    except requests.exceptions.RequestException as e:
        st.error(f"Upload failed: {str(e)}")
        return


# Streamlit UI
st.title("Video Uploader with Points Selection")

video_file = st.file_uploader("Choose a video file", type=["mp4", "avi", "mov", "mkv"])

if video_file:
    st.video(video_file)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as temp_video_file:
        temp_video_file.write(video_file.read())
        temp_video_path = temp_video_file.name
        st.write(f"Temporary video file saved at: {temp_video_path}")

    if 'points' not in st.session_state:
        st.session_state.points = []

    # Select video type
    video_type = st.selectbox("Select Video Type", ["People", "Vehicle"])

    if st.button("Select Points on Video"):
        points = select_points_on_video(temp_video_path)
        st.session_state.points = points
        st.write(f"Selected points: {points}")

    if st.button("Upload Video"):
        points = st.session_state.points

        if points:
            with st.spinner("Uploading..."):
                response = upload_video_and_points(video_file, points, video_type)

            if response is not None:  # This is the key fix
                if response.status_code == 200:
                    st.success("Video uploaded successfully!")
                else:
                    st.error(f"Upload failed: {response.status_code} - {response.text}")
            else:
                st.error("Upload failed: No response from server (backend may have crashed)")

