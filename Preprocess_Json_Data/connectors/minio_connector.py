import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
import json
from typing import Dict, Any, List
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

    def ensure_bucket_exists(self, bucket):
        """Ensure the bucket exists, create if it doesn't"""
        try:
            if not self.minio_client.bucket_exists(bucket):
                self.minio_client.make_bucket(bucket)
                logging.info(f"Created bucket: {bucket}")
            return True
        except S3Error as e:
            logging.error(f"MinIO bucket creation error: {e}")
            return False
        except Exception as e:
            logging.error(f"Error verifying buckets: {e}")
            return False
    
    def read_json(self, bucket: str, path: str, multiLine=True) -> DataFrame:
        """Read JSON data from MinIO"""
        self.ensure_bucket_exists(bucket)
        s3_path = f"s3a://{bucket}/{path}"
        return (self.spark.read
                .option("multiLine", multiLine)
                .option("mode", "PERMISSIVE")
                .json(s3_path))
    
    def write_json(self, df: DataFrame, bucket: str, path: str, mode: str = "overwrite"):
        """Write DataFrame as proper JSON array"""
        self.ensure_bucket_exists(bucket)
        
        # First write to a temporary location
        temp_path = f"s3a://{bucket}/_temp_{path}"
        df.write.mode(mode).json(temp_path)
        
        # Read the JSON files and combine into a single array
        json_df = self.spark.read.json(temp_path)
        json_data = json_df.toJSON().collect()
        
        # Create proper JSON array
        json_array = "[\n" + ",\n".join([json.dumps(json.loads(row), indent=4) for row in json_data]) + "\n]"

        # Write the combined JSON to the final location
        json_bytes = json_array.encode('utf-8')
        json_stream = BytesIO(json_bytes)
        
        self.minio_client.put_object(
            bucket,
            path,
            json_stream,
            length=len(json_bytes),
            content_type='application/json'
        )
        
        # Clean up temporary files
        try:
            objects = self.minio_client.list_objects(bucket, prefix=f"_temp_{path}")
            for obj in objects:
                self.minio_client.remove_object(bucket, obj.object_name)
        except S3Error as e:
            logging.error(f"Error cleaning up temp files: {e}")
   
    def write_single_json(self, data: Dict[str, Any], bucket: str, path: str):
        """Write Python dict as single JSON file"""
        self.ensure_bucket_exists(bucket)
        json_bytes = json.dumps(data).encode('utf-8')
        json_stream = BytesIO(json_bytes)
        
        self.minio_client.put_object(
            bucket,
            path,
            json_stream,
            length=len(json_bytes),
            content_type='application/json'
        )
    
    def list_json_files(self, bucket: str, folder: str = "") -> List[str]:
        """
        List all JSON files in a bucket folder
        Args:
            bucket: Name of the bucket
            folder: Folder path within the bucket (without leading/trailing slashes)
        Returns:
            List of file names (not full paths) with .json extension
        """
        try:
            objects = self.minio_client.list_objects(bucket, prefix=folder, recursive=True)
            json_files = [
                obj.object_name.split('/')[-1]  # Get just the filename
                for obj in objects 
                if obj.object_name.endswith('.json') and not obj.is_dir
            ]
            return json_files
        except S3Error as e:
            logging.error(f"Error listing JSON files in {bucket}/{folder}: {e}")
            return []
        except Exception as e:
            logging.error(f"Unexpected error listing JSON files: {e}")
            return []