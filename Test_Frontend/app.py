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



def upload_video_and_points(video_file, points_data, video_type):
    try:
        files = {"file": (video_file.name, video_file, "video/mp4")}

        if video_type == "People":
            for key in ["entry", "exit", "restricted"]:
                if key not in points_data:
                    points_data[key] = []
            points = {
                "entry": points_data["entry"],
                "exit": points_data["exit"],
                "restricted": points_data["restricted"]
            }
            url = "http://localhost:8011/upload_people"

        elif video_type == "Vehicle":
            if isinstance(points_data, list):
                points = points_data  # If points_data is a list for vehicles
            else:
                print(f"Error: Expected points_data to be a list for Vehicle type, but got {type(points_data)}.")
                return None
            url = "http://localhost:8012/upload_vehicle"

        else:
            raise ValueError("Invalid video type")

        data = {"points": json.dumps(points)}
        response = requests.post(url, files=files, data=data)
        return response

    except Exception as e:
        print(f"Error uploading video: {str(e)}")
        return None




# Streamlit UI
st.title("Video Uploader with Points Selection")

video_file = st.file_uploader("Choose a video file", type=["mp4", "avi", "mov", "mkv"])

if video_file:
    st.video(video_file)

    video_type = st.selectbox("Select Video Type", ["People", "Vehicle"])

    if 'points_data' not in st.session_state:
        st.session_state.points_data = {"Entry": [], "Exit": [], "Restricted": [], "Vehicle": []}

    if video_type == "People":
        option = st.radio("Select Point Type", ["Entry", "Exit", "Restricted"])
    else:
        option = "Vehicle"

    if st.button("Select Points on Video"):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
            tmp.write(video_file.read())
            tmp_path = tmp.name

        selected_points = select_points_on_video(tmp_path)
        os.remove(tmp_path)

        if selected_points:
            st.session_state.points_data[option] = [list(point) for point in selected_points]
            st.success(f"{option} points saved: {st.session_state.points_data[option]}")
        else:
            st.error("No points selected.")

    if st.button("Upload Video"):
        if video_type == "People":
            points_to_send = {
                "entry": st.session_state.points_data["Entry"],
                "exit": st.session_state.points_data["Exit"],
                "restricted": st.session_state.points_data["Restricted"]
            }
            valid = all(points_to_send.values())
            print("Valid ;", valid)
            
        else:
            points_to_send = st.session_state.points_data["Vehicle"]
            valid = bool(points_to_send)
            
        if valid:
            with st.spinner("Uploading..."):
                print("points to send",points_to_send)
                response = upload_video_and_points(video_file, points_to_send, video_type)
                print("Respomse", response)
                
            if response:
                if response.status_code == 200:
                    st.success("Video uploaded and processed successfully!")
                    st.json(response.json())  # optional: show returned data
                else:
                    st.error(f"Upload failed: {response.status_code} - {response.text}")
        else:
            st.warning("Please select all required points before uploading.")
