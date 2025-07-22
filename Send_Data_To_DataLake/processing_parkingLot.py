from datetime import datetime
import logging
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

def parkingLot_upload_to_minio(file_path):
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

        slot_summaries = data.get("slot_level_summary", [])
        if not slot_summaries:
            logging.warning("No slot-level summaries found in refined file.")
            raise ValueError("No parking data to upload.")

        es = Elasticsearch([ES_HOST])
        es.indices.create(index=ES_INDEX, ignore=400)

        now = datetime.utcnow()
        source_file = os.path.basename(file_path)
        processing_date = now.date().isoformat()
        processing_version = "v1.0"

        for i, slot in enumerate(slot_summaries):
            record = {
                "@timestamp": now.isoformat(),
                "source_file": source_file,
                "processing_date": processing_date,
                "processing_version": processing_version,
                "slot_id": slot.get("slot_id"),
                "slot_status": slot.get("slot_status"),
                "state_transitions": slot.get("state_transitions"),
                "time_metrics": slot.get("time_metrics"),
                "parking_sessions": slot.get("parking_sessions")
            }


            es.index(index=ES_INDEX, id=i + 1, body=record, pipeline="parking_data_timestamp_pipeline")
            print(f"Uploaded doc {i + 1} to Elasticsearch")

    except Exception as e:
        print(f"Error uploading parking data: {e}")
        raise




