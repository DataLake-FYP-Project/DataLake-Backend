import json
import os
from collections import defaultdict
from elasticsearch import Elasticsearch
import boto3
from datetime import datetime

# ==== MinIO config ====
MINIO_ENDPOINT = 'http://localhost:9000'
ACCESS_KEY = 'minioadmin'
SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'raw'

# ==== Elasticsearch config ====
ES_HOST = "http://localhost:9200"
ES_INDEX = "datalake-common-data"


# ==== Load Detection JSON ====
def load_detections(filepath):
    with open(filepath, "r") as f:
        return json.load(f)


# ==== Group by class_name ====
def group_by_class_name(detections):
    grouped = defaultdict(list)
    for item in detections:
        grouped[item["class_name"]].append(item)
    return grouped


# ==== Save grouped JSON files ====
def save_grouped_json(grouped_data, output_dir="Grouped"):
    os.makedirs(output_dir, exist_ok=True)
    file_paths = []
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    for class_name, objects in grouped_data.items():
        class_dir = os.path.join(output_dir, class_name)
        os.makedirs(class_dir, exist_ok=True)
        filename = f"{class_name}_{timestamp}.json"
        path = os.path.join(class_dir, filename)
        with open(path, "w") as f:
            json.dump(objects, f, indent=2)
        file_paths.append((class_name, path))

    return file_paths


def common_upload_to_minio(file_path):
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )
    try:
        folder_path = f"common_detection/"
        s3_key = f"{folder_path}{os.path.basename(file_path)}"
        s3.upload_file(file_path, BUCKET_NAME, s3_key)
        print(f"JSON File uploaded to MinIO: s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")


# ==== Upload to Elasticsearch ====
def common_upload_to_elasticsearch(file_path, class_name=None):
    es = Elasticsearch([ES_HOST])
    es.indices.create(index=ES_INDEX, ignore=400)

    with open(file_path, "r") as f:
        data = json.load(f)

    if isinstance(data, dict):
        data = [data]

    for i, record in enumerate(data):
        if isinstance(record, dict):
            # Optional: unwrap 'frame_data' if present
            record = record.get("frame_data", record)
            es.index(index=ES_INDEX, body=record, id=None, pipeline="common_data_timestamp_pipeline")
        else:
            print(f"Skipping non-dict record: {type(record)}")

    print(f"Uploaded {len(data)} records to Elasticsearch")


# ==== Main processing wrapper ====
def group_and_save_objects(json_path):
    """
    Combines loading, grouping, and saving detection objects into class-wise JSON files.

    Args:
        json_path (str): Path to the input JSON file with detections.

    Returns:
        List of (class_name, file_path) tuples
    """
    detections = load_detections(json_path)
    grouped = group_by_class_name(detections)
    grouped_paths = save_grouped_json(grouped)
    return grouped_paths
