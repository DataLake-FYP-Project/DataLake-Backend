import streamlit as st
import requests


# python -m streamlit run app.py

def upload_video(video_file):
    url = "http://localhost:8011/upload"
    files = {"file": (video_file.name, video_file, "video/mp4")}
    response = requests.post(url, files=files)
    return response

st.title("Video Uploader")

video_file = st.file_uploader("Choose a video file", type=["mp4", "avi", "mov", "mkv"])

if video_file:
    st.video(video_file)
    
    if st.button("Upload Video"):
        with st.spinner("Uploading..."):
            response = upload_video(video_file)
        
        if response.status_code == 200:
            st.success("Video uploaded successfully!")
        else:
            st.error(f"Upload failed: {response.status_code} - {response.text}")
