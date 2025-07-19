import boto3
import os
import json
from elasticsearch import Elasticsearch
from collections import defaultdict


# MinIO Connection Details
MINIO_ENDPOINT = 'http://127.0.0.1:9000'
ACCESS_KEY = 'minioadmin'
SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'raw'

# Elasticsearch Connection Details
ES_HOST = "http://localhost:9200"
ES_INDEX = "datalake-pose-data"

def convert_pose_json_format(input_path, output_path):
    with open(input_path, 'r') as file:
        data = json.load(file)

    frame_detections = data.get("frame_detections", [])
    flattened_frames = []

    for frame in frame_detections:
        flattened = {
            "frame_number": frame.get("frame_number"),
            "action": frame.get("action"),
            "confidence": frame.get("confidence"),
            "keypoints": frame.get("keypoints", [])
        }
        flattened_frames.append(flattened)

    with open(output_path, 'w') as f:
        json.dump(flattened_frames, f, indent=4)


def pose_upload_to_elasticsearch(file_path):
    try:
        with open(file_path, "r") as file:
            frames = json.load(file)

        if not frames or not isinstance(frames, list):
            print("Invalid pose frame data.")
            return

        es = Elasticsearch(["http://localhost:9200"])

        # Make sure index exists
        if not es.indices.exists(index="datalake-pose-data"):
            print("Index does not exist. Please create it first with correct mappings.")
            return

        # Upload each document one by one using the pipeline
        for i, frame in enumerate(frames):
            if not isinstance(frame, dict):
                print(f"Invalid frame format at index {i}")
                continue

            es.index(
                index="datalake-pose-data",
                id=i + 1,
                document=frame,
                pipeline="pose_data_timestamp_pipeline"
            )

        print(f"Successfully uploaded {len(frames)} pose frames to Elasticsearch.")

    except Exception as e:
        print(f"Error uploading pose frames to Elasticsearch: {e}")