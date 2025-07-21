from datetime import datetime
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
ES_INDEX = "datalake-parking-data"

def parking_upload_to_minio(file_path):
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )
    try:
        folder_path = f"parkingLot_detection/"
        s3_key = f"{folder_path}{os.path.basename(file_path)}"
        s3.upload_file(file_path, BUCKET_NAME, s3_key)
        print(f"JSON File uploaded to MinIO: s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")


def parking_upload_to_elasticsearch(file_path):
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        detections = data.get("frame_detections", [])
        if not detections:
            print("No detections found in refined file.")
            return

        es = Elasticsearch([ES_HOST])
        es.indices.create(index=ES_INDEX, ignore=400)

        now = datetime.utcnow()
        source_file = os.path.basename(file_path)
        processing_date = now.date().isoformat()
        processing_version = "v1.0"

        for i, detection in enumerate(detections):
            record = {
                "@timestamp": now.isoformat(),
                "source_file": source_file,
                "processing_date": processing_date,
                "processing_version": processing_version
            }

            # Add detection fields directly
            for key, value in detection.items():
                record[key] = value

            es.index(index=ES_INDEX, id=i + 1, body=record, pipeline="parking_data_timestamp_pipeline")
            print(f"Uploaded doc {i + 1} to Elasticsearch")

    except Exception as e:
        print(f"Error uploading parking data: {e}")




