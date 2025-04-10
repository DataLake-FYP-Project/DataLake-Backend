from pyspark.sql import DataFrame
import json
from typing import Dict, Any

class MinIOConnector:
    def __init__(self, spark):
        self.spark = spark
    
# In minio_connector.py, modify the read_json method:
    def read_json(self, bucket: str, path: str, multiLine=True) -> DataFrame:
        """Read JSON data from MinIO bucket with proper handling"""
        s3_path = f"s3a://{bucket}/{path}"
        return self.spark.read.option("multiLine", multiLine) \
                        .option("mode", "PERMISSIVE") \
                        .option("columnNameOfCorruptRecord", "_corrupt_record") \
                        .json(s3_path)
    
    def write_parquet(self, df: DataFrame, bucket: str, path: str, mode: str = "overwrite"):
        """Write DataFrame as Parquet to MinIO"""
        s3_path = f"s3a://{bucket}/{path}"
        df.write.mode(mode).parquet(s3_path)
    
    def write_json(self, df: DataFrame, bucket: str, path: str, mode: str = "overwrite"):
        """Write DataFrame as JSON to MinIO"""
        s3_path = f"s3a://{bucket}/{path}"
        df.write.mode(mode).json(s3_path)
    
    def write_single_json(self, data: Dict[str, Any], bucket: str, path: str):
        """Write Python dict as single JSON file to MinIO"""
        import io
        from minio import Minio
        from config.minio_config import MINIO_CONFIG
        
        minio_client = Minio(
            MINIO_CONFIG["endpoint"].replace("http://", ""),
            access_key=MINIO_CONFIG["access_key"],
            secret_key=MINIO_CONFIG["secret_key"],
            secure=MINIO_CONFIG["secure"]
        )
        
        json_bytes = json.dumps(data).encode('utf-8')
        json_stream = io.BytesIO(json_bytes)
        
        minio_client.put_object(
            bucket,
            path,
            json_stream,
            length=len(json_bytes),
            content_type='application/json'
        )