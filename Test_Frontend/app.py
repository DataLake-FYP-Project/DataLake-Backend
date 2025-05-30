import json
import streamlit as st
import requests
import cv2
import tempfile
import os


# Function to capture points on the video
def select_points_on_video(video_path, num_points=4, new_width=640, new_height=480):
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
        if event == cv2.EVENT_LBUTTONDOWN and len(points) < num_points:
            points.append((x, y))
            print(f"Point {len(points)}: ({x}, {y})")
            if len(points) == num_points:
                cv2.destroyAllWindows()

    cv2.imshow("Click to Select Points", frame_resized)
    cv2.setMouseCallback("Click to Select Points", click_event)

    while len(points) < num_points:
        cv2.waitKey(1)

    cv2.destroyAllWindows()
    cap.release()

    return scale_polygon_points(points, original_width, original_height, new_width, new_height)


def scale_polygon_points(polygon_points, original_width, original_height, new_width, new_height):
    scale_x = original_width / new_width
    scale_y = original_height / new_height
    return [(int(x * scale_x), int(y * scale_y)) for x, y in polygon_points]


def upload_video_and_points(video_file, points_data, video_type, metadata_to_send=None):
    try:
        files = {"file": (video_file.name, video_file, "video/mp4")}
        data = {}

        if video_type == "People":
            points = {
                "entry": points_data.get("entry", []),
                "exit": points_data.get("exit", []),
                "restricted": points_data.get("restricted", [])
            }
            url = "http://localhost:8011/upload_people"
            data["points"] = json.dumps(points)

        elif video_type == "Vehicle":
            points = {
                "Area": points_data.get("Area", []),
                "red_light": points_data.get("red_light", []),
                "line_points": points_data.get("line_points", [])
            }
            url = "http://localhost:8012/upload_vehicle"
            data["points"] = json.dumps(points)

        elif video_type == "Vehicle Geolocation tracker":
            url = "http://localhost:8012/upload_vehicle"
            data["metadata"] = json.dumps(metadata_to_send)

        elif video_type == "Safety":
            url = "http://localhost:8014/upload_safety"

        else:
            raise ValueError("Invalid video type")

        print("Sending to:", url)
        print("Data:", json.dumps(data, indent=2))

        response = requests.post(url, files=files, data=data)
        return response

    except Exception as e:
        print(f"Error uploading video: {str(e)}")
        return None


# Streamlit UI
st.title("Video Uploader with Points Selection")

video_file = st.file_uploader("Choose a video file", type=["mp4", "avi", "mov", "mkv"])

# Initialize session state
if 'points_data' not in st.session_state:
    st.session_state.points_data = {
        "Entry": [], "Exit": [], "Restricted": [],
        "Area": [], "red_light": [], "line_points": []
    }

if video_file:
    st.video(video_file)

    video_type = st.selectbox("Select Video Type", ["People", "Vehicle", "Vehicle Geolocation tracker", "Safety"])

    camera_metadata = None

    point_types_map = {
        "People": ["Entry", "Exit", "Restricted"],
        "Vehicle": ["Area", "red_light", "line_points"]
    }

    if video_type in point_types_map:
        point_types = point_types_map[video_type]
        option = st.radio("Select Point Type", point_types)

        if st.button("Select Points on Video"):
            with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
                tmp.write(video_file.read())
                tmp_path = tmp.name

            num_points = 2 if option == "line_points" else 4
            selected_points = select_points_on_video(tmp_path, num_points=num_points)
            os.remove(tmp_path)

            if selected_points:
                st.session_state.points_data[option] = [list(point) for point in selected_points]
                st.success(f"{option} points saved: {st.session_state.points_data[option]}")
            else:
                st.error("No points selected.")

        if st.button("Clear All Points"):
            st.session_state.points_data = {
                key: [] for key in set().union(*point_types_map.values())
            }
            st.success("All points cleared.")

    elif video_type == "Vehicle Geolocation tracker":
        st.subheader("Camera Metadata")
        camera_lat = st.number_input("Enter the camera's latitude", value=7.076772, step=0.0000001)
        camera_lon = st.number_input("Enter the camera's longitude", value=80.0443499, step=0.0000001)
        camera_heading = st.number_input("Enter the camera's heading in degrees", value=206.43, step=0.01)

        camera_metadata = {
            "latitude": camera_lat,
            "longitude": camera_lon,
            "heading": camera_heading
        }
        st.session_state.camera_metadata = camera_metadata

    if st.button("Upload Video"):
        valid = False
        points_to_send = {}
        metadata_to_send = {}

        if video_type == "People":
            entry_selected = bool(st.session_state.points_data.get("Entry"))
            exit_selected = bool(st.session_state.points_data.get("Exit"))
            restricted_selected = bool(st.session_state.points_data.get("Restricted"))

            points_to_send = {
                "entry": st.session_state.points_data.get("Entry") if entry_selected else [],
                "exit": st.session_state.points_data.get("Exit") if exit_selected else [],
                "restricted": st.session_state.points_data.get("Restricted") if restricted_selected else []
            }

            valid = entry_selected and exit_selected and restricted_selected

        elif video_type == "Vehicle":
            area_selected = bool(st.session_state.points_data.get("Area"))
            line_selected = bool(st.session_state.points_data.get("line_points"))
            red_selected = bool(st.session_state.points_data.get("red_light"))

            points_to_send = {
                "Area": st.session_state.points_data.get("Area") if area_selected else [],
                "red_light": st.session_state.points_data.get("red_light") if red_selected else [],
                "line_points": st.session_state.points_data.get("line_points") if line_selected else []
            }

            valid = area_selected

        elif video_type == "Vehicle Geolocation tracker":
            lat = st.session_state.camera_metadata.get("latitude")
            lon = st.session_state.camera_metadata.get("longitude")
            heading = st.session_state.camera_metadata.get("heading")

            metadata_to_send = {
                "latitude": lat,
                "longitude": lon,
                "heading": heading
            }

            valid = all([lat, lon, heading])

        elif video_type == "Safety":
            valid = True  # No point selection needed

        if valid:
            with st.spinner("Uploading..."):
                if video_type == "Vehicle Geolocation tracker":
                    response = upload_video_and_points(video_file, {}, video_type, metadata_to_send)
                else:
                    response = upload_video_and_points(video_file, points_to_send, video_type)

            if response:
                if response.status_code == 200:
                    st.success("Video uploaded and processed successfully!")
                    st.json(response.json())
                else:
                    st.error(f"Upload failed: {response.status_code} - {response.text}")
        else:
            st.warning("Please select all required inputs before uploading.")
