import boto3
from flask import Flask, request, jsonify
import os
import json
from elasticsearch import Elasticsearch
from collections import defaultdict

app = Flask(__name__)

# MinIO Connection Details
MINIO_ENDPOINT = 'http://127.0.0.1:9000'
ACCESS_KEY = 'minioadmin'
SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'vehicle-data'

# Elasticsearch Connection Details
ES_HOST = "http://localhost:9200"
ES_INDEX = "camera-footage"  


def get_max_percentage(attribute_values):
    """Helper function to get the most frequent value from a list of attributes."""
    value_counts = defaultdict(int)
    for value in attribute_values:
        value_counts[value] += 1

    max_value = max(value_counts, key=value_counts.get)
    return max_value

def get_average_confidence(confidences):
    """Helper function to get the average confidence from a list of confidence values."""
    return sum(confidences) / len(confidences) if confidences else 0

def process_tracker_data(data):
    """Process the data to get max percentage values for each tracker and average confidence."""
    tracker_data = defaultdict(lambda: {
        "class_id": [],
        # "class_name": [],
        # "vehicle_color": [],
        # "direction": [],
        # "lane": [],
        "confidence": []
    })

    # Collect data for each tracker_id
    for frame in data:
        for detection in frame["detections"]:
            tracker_id = detection["tracker_id"]
            tracker_data[tracker_id]["class_id"].append(detection["class_id"])
            # tracker_data[tracker_id]["class_name"].append(detection["class_name"])
            # tracker_data[tracker_id]["vehicle_color"].append(detection["vehicle_color"])
            # tracker_data[tracker_id]["direction"].append(detection["direction"])
            # tracker_data[tracker_id]["lane"].append(detection["lane"])
            tracker_data[tracker_id]["confidence"].append(detection["confidence"])

    # Prepare the output data with max values for each tracker_id
    output_data = []
    for tracker_id, attributes in tracker_data.items():
        output_entry = {
            "tracker_id": tracker_id,
            "class_id": get_max_percentage(attributes["class_id"]),
            # "class_name": get_max_percentage(attributes["class_name"]),
            # "vehicle_color": get_max_percentage(attributes["vehicle_color"]),
            # "direction": get_max_percentage(attributes["direction"]),
            # "lane": get_max_percentage(attributes["lane"]),
            "average_confidence": get_average_confidence(attributes["confidence"])
        }
        output_data.append(output_entry)

    return output_data


def convert_json_format(input_path, output_path):
    """Convert a JSON list of dictionaries into a newline-separated JSON format."""
    with open(input_path, "r") as f:
        data = json.load(f) 

    output_data = process_tracker_data(data)

    with open(output_path, "w") as f:
        json.dump(output_data, f, indent=4)

    print(f"Transformed JSON saved to {output_path}")






def upload_to_minio(file_path, video_name):
    """Uploads a JSON file to MinIO inside a specific folder based on the video name."""
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

def upload_to_elasticsearch(file_path):
    """Reads JSON file and uploads its contents to Elasticsearch."""
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
        es = Elasticsearch([ES_HOST])

        es.indices.create(index=ES_INDEX, ignore=400)

        if isinstance(data, list):
            for i, record in enumerate(data):
                res = es.index(index=ES_INDEX, id=i + 1, body=record)
                print(f"Document {i + 1} uploaded to Elasticsearch: {res['result']}")
        else:
            res = es.index(index=ES_INDEX, id=1, body=data)
            print(f"Single document uploaded to Elasticsearch: {res['result']}")
    except Exception as e:
        print(f"Error uploading to Elasticsearch: {e}")

@app.route("/upload_2", methods=["POST"])
def upload_json():
    """Receives JSON file path from first backend and uploads its contents."""
    if "json_file" not in request.files:
        return jsonify({"error": "No JSON file uploaded"}), 400

    json_file = request.files["json_file"]
    if json_file.filename == "":
        return jsonify({"error": "No selected file"}), 400

    
    video_name = json_file.filename.split('.')[0]

    json_folder = "JSON_FOLDER"
    if not os.path.exists(json_folder):
        os.makedirs(json_folder)
        
    json_path = os.path.join(json_folder, json_file.filename)
    json_file.save(json_path)
    
    processed_json = "processed.json"
    convert_json_format(json_path, processed_json)

    upload_to_minio(json_path, video_name)
    upload_to_minio(processed_json, video_name)
    # upload_to_elasticsearch(processed_json)

    return jsonify({"message": "File uploaded successfully"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8012, debug=True)
