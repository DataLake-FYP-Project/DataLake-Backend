import json
from typing import Any, Dict
import boto3
from Preprocess_Json_Data.config.minio_config import MINIO_CONFIG


class MinIOConnector:
    def __init__(self, spark):
        self.spark = spark
        self.config = MINIO_CONFIG

        # Fix endpoint if it doesn't already have http/https
        if not self.config["endpoint"].startswith("http"):
            scheme = "https" if self.config.get("secure", False) else "http"
            self.config["endpoint"] = f"{scheme}://{self.config['endpoint']}"

        # Create reusable boto3 client
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.config["endpoint"],
            aws_access_key_id=self.config["access_key"],
            aws_secret_access_key=self.config["secret_key"],
        )

    def fetch_json(self, bucket: str, object_path: str) -> Dict[str, Any]:
        """Download JSON file from MinIO"""
        try:
            obj = self.s3_client.get_object(Bucket=bucket, Key=object_path)
            return json.loads(obj["Body"].read())
        except Exception as e:
            raise RuntimeError(f"Failed to fetch JSON from MinIO: {e}")

    def write_single_json(self, data: Dict[str, Any], bucket: str, object_path: str):
        """Upload JSON file to MinIO"""
        try:
            self.s3_client.put_object(
                Bucket=bucket,
                Key=object_path,
                Body=json.dumps(data, indent=2),
                ContentType="application/json"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to write JSON to MinIO: {e}")
