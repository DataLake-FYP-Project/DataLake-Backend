import boto3
import os
import json
from elasticsearch import Elasticsearch

# MinIO Connection Details
MINIO_ENDPOINT = 'http://127.0.0.1:9000'
ACCESS_KEY = 'minioadmin'
SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'camera-footage'
FOLDER_NAME = 'Raw_Json_File'
PROCESSED_LOCAL_FILE = "Processed_Json_File/frame_data.json"
UNPROCESSED_LOCAL_FILE = "Processed_Json_File/frame_data.json"

# Elasticsearch Connection Details
ES_HOST = "http://localhost:9200"
ES_INDEX = "camera-footage"  

def upload_to_minio():
    """Uploads a JSON file to MinIO inside a specific folder."""
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

    try:
        s3_key = f"{FOLDER_NAME}/{os.path.basename(UNPROCESSED_LOCAL_FILE)}"
        s3.upload_file(UNPROCESSED_LOCAL_FILE, BUCKET_NAME, s3_key)
        print(f"✅ JSON File uploaded to MinIO: s3://{BUCKET_NAME}/{s3_key}")

    except Exception as e:
        print(f"❌ Error uploading to MinIO: {e}")

def upload_to_elasticsearch():
    """Reads JSON file and uploads its contents to Elasticsearch."""
    try:
        with open(PROCESSED_LOCAL_FILE, "r") as file:
            data = json.load(file)
        es = Elasticsearch([ES_HOST])

        if isinstance(data, list):
            for i, record in enumerate(data):
                res = es.index(index=ES_INDEX, id=i + 1, body=record)
                print(f"✅ Document {i + 1} uploaded to Elasticsearch:", res["result"])
        else:
            res = es.index(index=ES_INDEX, id=1, body=data)
            print(f"✅ Single document uploaded to Elasticsearch:", res["result"])

    except Exception as e:
        print(f"❌ Error uploading to Elasticsearch: {e}")

upload_to_minio()
upload_to_elasticsearch()
