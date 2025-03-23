import boto3
from flask import Flask, request, jsonify
import os
import json
from elasticsearch import Elasticsearch

app = Flask(__name__)

# MinIO Connection Details
MINIO_ENDPOINT = 'http://127.0.0.1:9000'
ACCESS_KEY = 'minioadmin'
SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'camera-footage'
FOLDER_NAME = 'Raw_Json_File'

# Elasticsearch Connection Details
ES_HOST = "http://localhost:9200"
ES_INDEX = "camera-footage"  


def upload_to_minio(file_path):
    """Uploads a JSON file to MinIO inside a specific folder."""
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )
    try:
        s3_key = f"{FOLDER_NAME}/{os.path.basename(file_path)}"
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

    json_path = os.path.join("JSON_FOLDER", json_file.filename)
    json_file.save(json_path)
            
    # Upload to MinIO and Elasticsearch
    upload_to_minio(json_path)
    upload_to_elasticsearch(json_path)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8012, debug=True)
