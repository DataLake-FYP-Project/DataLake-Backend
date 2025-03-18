from minio import Minio
import json
from elasticsearch import Elasticsearch

# Connect to MinIO
minio_client = Minio(
    "localhost:9000",  # Change if MinIO runs elsewhere
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Define bucket and file name
bucket_name = "camera-footage"
json_file_name = "frame_data.json"

# Download JSON file
minio_client.fget_object(bucket_name, json_file_name, "local_footage_data.json")

# Load JSON
with open("local_footage_data.json", "r") as file:
    data = json.load(file)

# print("Data fetched from MinIO:", data[:2])  # Print first two entries for validation

# Connect to Elasticsearch
es = Elasticsearch("http://localhost:9200")  # Default Elasticsearch URL

# Define Index Name
index_name = "camera-footage"

# Insert Data into Elasticsearch
for i, record in enumerate(data):
    es.index(index=index_name, id=i+1, body=record)

print("Data successfully inserted into Elasticsearch!")

