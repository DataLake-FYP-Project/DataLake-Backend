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
        "vehicle_type": [],
        "vehicle_color": [],
        "vehicle_direction": [],
        "vehicle_lane": [],
        "confidence": [],
        "vehicle_speed": []
    })

    for frame in data:
        for detection in frame.get("detections", []):
            tracker_id = detection.get("tracker_id")
            if tracker_id is None:
                continue  # Skip detection if no tracker_id

            if "class_id" in detection:
                tracker_data[tracker_id]["class_id"].append(detection["class_id"])
            if "vehicle_type" in detection:
                tracker_data[tracker_id]["vehicle_type"].append(detection["vehicle_type"])
            if "vehicle_color" in detection:
                tracker_data[tracker_id]["vehicle_color"].append(detection["vehicle_color"])
            if "vehicle_direction" in detection:
                tracker_data[tracker_id]["vehicle_direction"].append(detection["vehicle_direction"])
            if "vehicle_lane" in detection:
                tracker_data[tracker_id]["vehicle_lane"].append(detection["vehicle_lane"])
            if "confidence" in detection:
                tracker_data[tracker_id]["confidence"].append(detection["confidence"])
            if "vehicle_speed" in detection:
                tracker_data[tracker_id]["vehicle_speed"].append(detection["vehicle_speed"])

    output_data = []
    for tracker_id, attributes in tracker_data.items():
        output_entry = {
            "tracker_id": tracker_id,
            "class_id": get_max_percentage(attributes["class_id"]),
            "vehicle_type": get_max_percentage(attributes["vehicle_type"]),
            "vehicle_color": get_max_percentage(attributes["vehicle_color"]),
            "vehicle_direction": get_max_percentage(attributes["vehicle_direction"]),
            "vehicle_lane": get_max_percentage(attributes["vehicle_lane"]),
            "average_confidence": get_average_confidence(attributes["confidence"]),
            "average_speed": get_average_speed(attributes["vehicle_speed"])
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

def vehicle_upload_to_minio(file_path):
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )
    try:
        folder_path = f"vehicle_detection/"
        s3_key = f"{folder_path}{os.path.basename(file_path)}"
        s3.upload_file(file_path, BUCKET_NAME, s3_key)
        print(f"JSON File uploaded to MinIO: s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")


def parse_vehicle_data(file_path):
    try:
        with open(file_path, "r") as file:
            data = json.load(file)

        vehicles = data.get("vehicles", {})
        parsed_records = []

        for vehicle_id_str, vehicle_info in vehicles.items():
            vehicle_info["vehicle_id"] = int(vehicle_id_str)  # Convert to int
            parsed_records.append(vehicle_info)

        return parsed_records

    except Exception as e:
        print(f"Error parsing JSON file: {e}")
        return []

def vehicle_upload_to_elasticsearch(file_path):
    try:
        vehicle_records = parse_vehicle_data(file_path)
        if not vehicle_records:
            print("No vehicle data to upload.")
            return

        es = Elasticsearch([ES_HOST])
        es.indices.create(index=ES_INDEX, ignore=400)

        for i, record in enumerate(vehicle_records):
            res = es.index(index=ES_INDEX, id=i + 1, body=record, pipeline="vehicle_data_timestamp_pipeline")
            print(f"Document {i + 1} uploaded to Elasticsearch: {res['result']}")

    except Exception as e:
        print(f"Error uploading to Elasticsearch: {e}")