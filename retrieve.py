# Retrieve files from buckets
import boto3

# MinIO Connection Details
ENDPOINT = 'http://127.0.0.1:9000'
ACCESS_KEY = 'minioadmin'
SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'raw'  # Bucket to download from

# Connect to MinIO
s3 = boto3.client(
    's3',
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

# Get the filename from the user
file_name = input("Enter the name of the file to download from the bucket: ")

# Download JSON File from MinIO
output_file = f"downloaded_{file_name}"  # Prefix the downloaded file with "downloaded_"
try:
    s3.download_file(BUCKET_NAME, file_name, output_file)
    print(f"File '{file_name}' downloaded as '{output_file}'")

    # List Objects in the Bucket
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)
    if 'Contents' in response:
        for obj in response['Contents']:
            print(f"File: {obj['Key']}, Size: {obj['Size']} bytes")
    else:
        print(f"Bucket '{BUCKET_NAME}' is empty.")

except Exception as e:
    print(f"Error downloading file: {e}")
