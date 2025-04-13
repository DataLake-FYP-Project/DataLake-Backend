import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
 
MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT"),
    "access_key": os.getenv("MINIO_ACCESS_KEY"),
    "secret_key": os.getenv("MINIO_SECRET_KEY"),
    "secure": os.getenv("MINIO_SECURE").lower() == "true"
}

BUCKETS = {
    "raw": os.getenv("RAW_BUCKET"),
    "processed": os.getenv("PROCESSED_BUCKET")
}


