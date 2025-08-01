import json
import streamlit as st
import requests
import cv2
import tempfile
import os
import pickle


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


# # Function to slot selection for parking on the video
def draw_parking_slots(video_path):
    cap = cv2.VideoCapture(video_path)
    success, frame = cap.read()
    cap.release()

    if not success:
        st.error("Could not read the video.")
        return None

    clone = frame.copy()
    drawing = False
    ix, iy = -1, -1
    slots = []
    window_closed = False

    def draw_rectangle(event, x, y, flags, param):
        nonlocal ix, iy, drawing, slots, clone, frame
        if event == cv2.EVENT_LBUTTONDOWN:
            drawing = True
            ix, iy = x, y
        elif event == cv2.EVENT_MOUSEMOVE:
            if drawing:
                temp = clone.copy()
                cv2.rectangle(temp, (ix, iy), (x, y), (255, 0, 0), 2)
                for sx, sy, sw, sh in slots:
                    cv2.rectangle(temp, (sx, sy), (sx + sw, sy + sh), (0, 255, 0), 1)
                cv2.imshow("Draw Parking Slots", temp)
        elif event == cv2.EVENT_LBUTTONUP:
            drawing = False
            x1, y1 = min(ix, x), min(iy, y)
            w, h = abs(x - ix), abs(y - iy)
            slots.append((x1, y1, w, h))
            cv2.rectangle(frame, (x1, y1), (x1 + w, y1 + h), (0, 255, 0), 2)
            cv2.imshow("Draw Parking Slots", frame)

    cv2.namedWindow("Draw Parking Slots", cv2.WINDOW_NORMAL)
    cv2.setMouseCallback("Draw Parking Slots", draw_rectangle)
    cv2.imshow("Draw Parking Slots", frame)

    st.info("\nClick and drag to draw parking slots. Close the window when finished.\n")

    # Get the current script's directory (folder A)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    save_path = os.path.join(parent_dir, "Create_Json_Data", "parking_service", "uploads", "parking_slot_coords.pkl")
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    while True:
        key = cv2.waitKey(1) & 0xFF
        # Check if window was closed
        if cv2.getWindowProperty("Draw Parking Slots", cv2.WND_PROP_VISIBLE) < 1:
            break

    if slots:  # Only save if we have slots
        with open(save_path, "wb") as f:
            pickle.dump(slots, f)
        print(f"Saved {len(slots)} slots to {save_path}")
        return save_path
    else:
        print("No slots were saved")
        return None


def upload_video_and_points(video_file, points_data, video_type, metadata_to_send=None):
    try:
        video_file.seek(0)
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

        elif video_type == "Pose":
            url = "http://localhost:8015/upload_pose"
        
        elif video_type == "Animal":
            url = "http://localhost:8016/upload_animal"

        elif video_type == "Parking":
            url = "http://localhost:8017/upload_parking"
        
        elif video_type == "Mask_Parking":
            url = "http://localhost:8018/upload_mask_parking"

        elif video_type == "Common":
            url = "http://localhost:8019/upload_common"

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

    video_type = st.selectbox("Select Video Type", ["People", "Vehicle", "Vehicle Geolocation tracker", "Safety", "Pose","Animal","Common", "Parking"])

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

    elif video_type == "Parking":
        st.subheader("Parking Slot Selection Method")
        
        # Radio button to choose between manual and automatic
        selection_method = st.radio(
            "Choose slot selection method:",
            ("Automatically detect slots (using mask)", "Manually draw slots")
        )

        if selection_method == "Manually draw slots":
            if st.button("Draw Parking Slots"):
                with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
                    tmp.write(video_file.read())
                    tmp_path = tmp.name

                save_path = draw_parking_slots(tmp_path)
                os.remove(tmp_path)

                if save_path:
                    st.success(f"Saved parking slot coordinates to: {save_path}")
                else:
                    st.warning("No slots saved.")
        else:
            video_type = "Mask_Parking"

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

        elif video_type in ["Safety", "Pose", "Animal", "Parking","Mask_Parking"]:
            valid = True

        elif video_type == "Common":
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
