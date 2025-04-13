import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
import json
from typing import Dict, Any
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import os
from config.minio_config import MINIO_CONFIG, BUCKETS

class MinIOConnector:
    def __init__(self, spark):
        self.spark = spark
        self.minio_client = Minio(
            MINIO_CONFIG["endpoint"].replace("http://", ""),
            access_key=MINIO_CONFIG["access_key"],
            secret_key=MINIO_CONFIG["secret_key"],
            secure=MINIO_CONFIG["secure"]
        )

    def _ensure_bucket_exists(self, bucket_name):
        """Ensure the bucket exists, create if it doesn't"""
        try:
            if not self.minio_client.bucket_exists(bucket_name):
                self.minio_client.make_bucket(bucket_name)
                logging.info(f"Created bucket: {bucket_name}")
            return True
        except S3Error as e:
            logging.error(f"MinIO error ensuring bucket exists: {e}")
            return False
    
    def read_json(self, bucket: str, path: str, multiLine=True) -> DataFrame:
        """Read JSON data from MinIO"""
        self._ensure_bucket_exists(bucket)
        s3_path = f"s3a://{bucket}/{path}"
        return (self.spark.read
                .option("multiLine", multiLine)
                .option("mode", "PERMISSIVE")
                .json(s3_path))
    
    def write_parquet(self, df: DataFrame, bucket: str, path: str, mode: str = "overwrite"):
        """Write DataFrame as Parquet"""
        self._ensure_bucket_exists(bucket)
        s3_path = f"s3a://{bucket}/{path}"
        df.write.mode(mode).parquet(s3_path)
    
    def write_json(self, df: DataFrame, bucket: str, path: str, mode: str = "overwrite"):
        """Write DataFrame as JSON"""
        self._ensure_bucket_exists(bucket)
        s3_path = f"s3a://{bucket}/{path}"
        df.write.mode(mode).json(s3_path)
    
    def write_single_json(self, data: Dict[str, Any], bucket: str, path: str):
        """Write Python dict as single JSON file"""
        self._ensure_bucket_exists(bucket)
        json_bytes = json.dumps(data).encode('utf-8')
        json_stream = BytesIO(json_bytes)
        
        self.minio_client.put_object(
            bucket,
            path,
            json_stream,
            length=len(json_bytes),
            content_type='application/json'
        )

        