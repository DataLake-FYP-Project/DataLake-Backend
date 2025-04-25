import boto3
import os

#  MinIO Connection Details
ENDPOINT = 'http://127.0.0.1:9000'
ACCESS_KEY = 'minioadmin'
SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'raw'  #  Use only "raw" as the bucket name (MinIO does not allow subdirectories as bucket names)
FOLDER_NAME = 'vehicle_detection'  # Folder inside the bucket
LOCAL_FILE = "lake_code/vehicle_tracking.json"  #  JSON file to upload

#  Connect to MinIO (S3-compatible API)
s3 = boto3.client(
    's3',
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

try:
    #  Ensure that we upload the file inside the "vehicle_detection" folder inside "raw" bucket
    s3_key = f"{FOLDER_NAME}/{os.path.basename(LOCAL_FILE)}"  # Saves inside `raw/vehicle_detection/`
    
    #  Upload JSON File to MinIO inside `raw/vehicle_detection/`
    s3.upload_file(LOCAL_FILE, BUCKET_NAME, s3_key)
    print(f" File '{LOCAL_FILE}' uploaded to 's3://{BUCKET_NAME}/{s3_key}'")

    # List objects inside `raw/vehicle_detection/`
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=FOLDER_NAME + "/")
    
    if 'Contents' in response:
        print("\n Files inside `raw/vehicle_detection/` in MinIO:")
        for obj in response['Contents']:
            print(f" {obj['Key']} ({obj['Size']} bytes)")
    else:
        print(f"\n No files found in `raw/vehicle_detection/`.")

except Exception as e:
    print(f" Error uploading file: {e}")
