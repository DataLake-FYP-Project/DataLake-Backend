import boto3
import os
import json
from elasticsearch import Elasticsearch
from collections import defaultdict

# MinIO Connection Details
MINIO_ENDPOINT = 'http://127.0.0.1:9000'
ACCESS_KEY = 'minioadmin'
SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'vehicle-data'

# Elasticsearch Connection Details
ES_HOST = "http://localhost:9200"
ES_INDEX = "datalake-vehicle-data"


def get_max_percentage(attribute_values):
    value_counts = defaultdict(int)
    for value in attribute_values:
        value_counts[value] += 1
    max_value = max(value_counts, key=value_counts.get)
    return max_value

def get_average_confidence(confidences):
    return sum(confidences) / len(confidences) if confidences else 0

def get_average_speed(speeds):
    return sum(speeds) / len(speeds) if speeds else 0

def process_tracker_data(data):
    tracker_data = defaultdict(lambda: {
        "class_id": [],
        "class_name": [],
        "vehicle_color": [],
        "direction": [],
        "lane": [],
        "confidence": [],
        "speed": []
    })

    for frame in data:
        for detection in frame["detections"]:
            tracker_id = detection["tracker_id"]
            tracker_data[tracker_id]["class_id"].append(detection["class_id"])
            tracker_data[tracker_id]["class_name"].append(detection["class_name"])
            tracker_data[tracker_id]["vehicle_color"].append(detection["vehicle_color"])
            tracker_data[tracker_id]["direction"].append(detection["direction"])
            tracker_data[tracker_id]["lane"].append(detection["lane"])
            tracker_data[tracker_id]["confidence"].append(detection["confidence"])
            tracker_data[tracker_id]["speed"].append(detection["speed"])

    output_data = []
    for tracker_id, attributes in tracker_data.items():
        output_entry = {
            "tracker_id": tracker_id,
            "class_id": get_max_percentage(attributes["class_id"]),
            "class_name": get_max_percentage(attributes["class_name"]),
            "vehicle_color": get_max_percentage(attributes["vehicle_color"]),
            "direction": get_max_percentage(attributes["direction"]),
            "lane": get_max_percentage(attributes["lane"]),
            "average_confidence": get_average_confidence(attributes["confidence"]),
            "average_speed": get_average_speed(attributes["speed"])
        }
        output_data.append(output_entry)

    return output_data

def convert_json_format(input_path, output_path):
    with open(input_path, "r") as f:
        data = json.load(f)

    output_data = process_tracker_data(data)

    with open(output_path, "w") as f:
        json.dump(output_data, f, indent=4)

    print(f"Transformed JSON saved to {output_path}")

def vehicle_upload_to_minio(file_path, video_name):
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )
    try:
        folder_path = f"{video_name}/"
        s3_key = f"{folder_path}{os.path.basename(file_path)}"
        s3.upload_file(file_path, BUCKET_NAME, s3_key)
        print(f"JSON File uploaded to MinIO: s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")

def vehicle_upload_to_elasticsearch(file_path):
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
            res = es.index(index=ES_INDEX, id=1, body=data, pipeline="add_timestamp")
            print(f"Single document uploaded to Elasticsearch: {res['result']}")
    except Exception as e:
        print(f"Error uploading to Elasticsearch: {e}")
