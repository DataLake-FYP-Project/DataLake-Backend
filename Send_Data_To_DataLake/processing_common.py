import json
import os
from collections import defaultdict
from elasticsearch import Elasticsearch
import boto3

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
    for class_name, objects in grouped_data.items():
        path = os.path.join(output_dir, f"{class_name}.json")
        with open(path, "w") as f:
            json.dump(objects, f, indent=2)
        file_paths.append((class_name, path))
    return file_paths


def common_upload_to_minio(local_file_path, class_name):
    client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    # Save under: common_detection/<class_name>/<class_name>.json
    key = f"common_detection/{class_name}/{class_name}.json"
    client.upload_file(local_file_path, BUCKET_NAME, key, ExtraArgs={"ContentType": "application/json"})
    print(f"Uploaded to MinIO: s3://{BUCKET_NAME}/{key}")


# ==== Upload to Elasticsearch ====
def common_upload_to_elasticsearch(file_path, class_name):
    es = Elasticsearch([ES_HOST])
    es.indices.create(index=ES_INDEX, ignore=400)

    with open(file_path, "r") as f:
        data = json.load(f)

    for i, record in enumerate(data):
        es.index(index=ES_INDEX, body=record, id=None, pipeline="common_data_timestamp_pipeline")
    print(f"✅ Uploaded to Elasticsearch: {class_name}.json")


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

