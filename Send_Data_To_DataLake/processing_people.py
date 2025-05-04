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
ES_INDEX = "datalake-people-data"

def convert_people_json_format(input_path, output_path):
    with open(input_path, 'r') as file:
        data = json.load(file)

    detections_dict = data.get("detections", {})
    flattened_detections = []

    for tracker_id, details in detections_dict.items():
        details["tracker_id"] = int(tracker_id)  # make sure tracker_id is an integer
        flattened_detections.append(details)

    with open(output_path, 'w') as f:
        json.dump(flattened_detections, f, indent=4)



def people_upload_to_minio(file_path, video_name):
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )
    try:
        folder_path = f"people_detection/{video_name}/"
        s3_key = f"{folder_path}{os.path.basename(file_path)}"
        s3.upload_file(file_path, BUCKET_NAME, s3_key)
        print(f"JSON File uploaded to MinIO: s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")

def people_upload_to_elasticsearch(file_path):
    try:
        with open(file_path, "r") as file:
            data = json.load(file)

        es = Elasticsearch([ES_HOST])
        es.indices.create(index=ES_INDEX, ignore=400)

        if isinstance(data, list):
            for i, record in enumerate(data):
                res = es.index(index=ES_INDEX, id=i + 1, body=record, pipeline="vehicle_data_timestamp_pipeline")
                print(f"Document {i + 1} uploaded to Elasticsearch: {res['result']}")
        else:
            res = es.index(index=ES_INDEX, id=1, body=data, pipeline="people_data_timestamp_pipeline")
            print(f"Single document uploaded to Elasticsearch: {res['result']}")

    except Exception as e:
        print(f"Error uploading to Elasticsearch: {e}")