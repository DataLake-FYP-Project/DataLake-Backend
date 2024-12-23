#upload json file to raw bucket

import boto3

# MinIO Connection Details
ENDPOINT = 'http://127.0.0.1:9000'
ACCESS_KEY = 'minioadmin'
SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'raw'  # Bucket to upload to

# Connect to MinIO
s3 = boto3.client(
    's3',
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

# Get JSON File Name from User
file_name = input("Enter the full path of the JSON file to upload: ")

try:
    # Upload JSON File
    s3.upload_file(file_name, BUCKET_NAME, file_name.split('\\')[-1])  # Only the filename is used as the key
    print(f"File '{file_name}' uploaded to bucket '{BUCKET_NAME}'")

    # List Objects in a Bucket
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)
    if 'Contents' in response:
        for obj in response['Contents']:
            print(f"File: {obj['Key']}, Size: {obj['Size']} bytes")
    else:
        print(f"Bucket '{BUCKET_NAME}' is empty.")

except Exception as e:
    print(f"Error uploading file: {e}")
