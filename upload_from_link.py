import io
from minio import Minio
from minio.error import S3Error
import requests
import os

def initialize_minio_client(endpoint, access_key, secret_key):
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

def upload_file(minio_client, bucket_name, file_path, object_name):
    try:
        minio_client.fput_object(bucket_name, object_name, file_path)
        print(f"File {file_path} uploaded to bucket {bucket_name} as {object_name}.")
    except S3Error as e:
        print(f"Failed to upload file: {e}")

def upload_from_url(minio_client, bucket_name, url, object_name):
    response = requests.get(url)
    if response.status_code == 200:
        data = response.content
        file_size = len(data)
        minio_client.put_object(bucket_name, object_name, io.BytesIO(data), file_size)
        print(f"File from {url} uploaded to bucket {bucket_name} as {object_name}.")
    else:
        print(f"Failed to download file from URL: {url}")

def main():
    endpoint = "127.0.0.1:9000"  # Removed 'http://'
    access_key = "minioadmin"
    secret_key = "minioadmin"
    bucket_name = "raw"

    # Initialize MinIO client
    minio_client = initialize_minio_client(endpoint, access_key, secret_key)

    # Example: Upload a file from local system
    file_path = "raw_data.json"
    object_name = os.path.basename(file_path)
    upload_file(minio_client, bucket_name, file_path, object_name)

    # Example: Upload a file from a URL
    url = "https://raw.githubusercontent.com/public-apis/public-apis/master/README.md"
    object_name = "public_apis_readme.md"
    upload_from_url(minio_client, bucket_name, url, object_name)

if __name__ == "__main__":
    main()
