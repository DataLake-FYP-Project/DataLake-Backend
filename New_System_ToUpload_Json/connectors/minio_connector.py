import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
import json
from typing import Dict, Any, List, Optional
from minio import Minio
from minio.error import S3Error
from io import BytesIO
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

    def write_json(self, df: DataFrame, bucket: str, path: str, mode: str = "overwrite", temp_bucket: Optional[str] = None):
        temp_bucket = temp_bucket or bucket
        """Write DataFrame as proper JSON array to MinIO"""
        self.ensure_bucket_exists(bucket)

        # Step 1: Write to a temporary location in MinIO (as partitioned JSON files)
        temp_path = f"s3a://{bucket}/_temp_{path}"
        df.write.mode(mode).json(temp_path)

        # Step 2: Read and collect JSON rows from temp path
        json_df = self.spark.read.json(temp_path)
        json_rows = json_df.toJSON().collect()  # list of valid JSON strings

        # Step 3: Convert to a proper JSON array (without re-dumping strings!)
        json_array = "[\n" + ",\n".join(json_rows) + "\n]"
        # wrapped_json = f'{{\n  "frame_detections": {json_array}\n}}'
        # json_bytes = wrapped_json.encode('utf-8')
        json_bytes = json_array.encode('utf-8')

        json_stream = BytesIO(json_bytes)

        self.minio_client.put_object(
            bucket,
            path,
            json_stream,
            length=len(json_bytes),
            content_type='application/json'
        )

        # Step 5: Clean up temporary files
        try:
            objects = self.minio_client.list_objects(bucket, prefix=f"_temp_{path}")
            for obj in objects:
                self.minio_client.remove_object(bucket, obj.object_name)
        except S3Error as e:
            logging.error(f"Error cleaning up temp files: {e}")

    def write_wrapped_json(self, df: DataFrame, bucket: str, path: str, key: str = "frame_detections"):
        """Wraps DataFrame content under a top-level key and writes to MinIO as a single object."""
        # from io import BytesIO

        # self.ensure_bucket_exists(bucket)

        # json_rows = df.toJSON().collect()
        # wrapped = f'{{\n  "{key}": [\n' + ",\n".join(json_rows) + '\n  ]\n}}'
        # wrapped = f'{{\n  "{key}": [\n' + ",\n".join(json_rows) + '\n  ]\n}'
        # json_bytes = wrapped.encode('utf-8')
        # stream = BytesIO(json_bytes)
        
        import json
        from io import BytesIO
        print("111111")
        self.ensure_bucket_exists(bucket)
        print("22222")

        json_rows = df.toJSON().collect()
        # Convert each row string to a dict, then wrap in a top-level key
        data = {key: [json.loads(row) for row in json_rows]}
        json_str = json.dumps(data, indent=2, ensure_ascii=False)
        json_bytes = json_str.encode('utf-8')
        stream = BytesIO(json_bytes)
        print("33333")
        self.minio_client.put_object(
            bucket,
            path,
            stream,
            len(json_bytes),
            content_type="application/json"
        )

    def write_single_json(self, data: Dict[str, Any], bucket: str, path: str):
        """Write Python dict as single formatted JSON file"""
        self.ensure_bucket_exists(bucket)

        # Pretty-print the dict into JSON with indentation
        json_str = json.dumps(data, indent=4, ensure_ascii=False)
        json_bytes = json_str.encode('utf-8')
        json_stream = BytesIO(json_bytes)

        self.minio_client.put_object(
            bucket,
            path,
            json_stream,
            length=len(json_bytes),
            content_type='application/json'
        )

    def write_json_string(self, json_str: str, bucket: str, path: str):
        """Write a JSON string directly to MinIO"""
        self.ensure_bucket_exists(bucket)
        json_bytes = json_str.encode('utf-8')
        json_stream = BytesIO(json_bytes)

        self.minio_client.put_object(
            bucket,
            path,
            json_stream,
            length=len(json_bytes),
            content_type='application/json'
        )

    def get_json_file(self, bucket: str, file_path: str) -> Optional[str]:
        """
        Retrieve a specific JSON file by exact path.
        
        Args:
            bucket: Name of the bucket
            file_path: Full path of the file (e.g., 'vehicle/test.json')
        
        Returns:
            The filename if it exists, otherwise None
        """
        try:
            obj = self.minio_client.stat_object(bucket, file_path)
            if obj:
                return file_path.split("/")[-1]
            return None
        except S3Error as e:
            logging.warning(f"JSON file '{file_path}' not found in bucket '{bucket}': {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error fetching JSON file: {e}")
            return None


    
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

    def fetch_json(self, bucket: str, path: str) -> Dict[str, Any]:
        """
        Fetch a specific JSON file from a MinIO bucket and return it as a dictionary.

        Args:
            bucket: Name of the bucket
            path: Path to the JSON file within the bucket (e.g., 'data/file.json')

        Returns:
            Dictionary containing the parsed JSON data

        Raises:
            S3Error: If there’s an error accessing the file in MinIO
            json.JSONDecodeError: If the file content is not valid JSON
            Exception: For other unexpected errors
        """
        try:
            # Ensure the bucket exists
            if not self.ensure_bucket_exists(bucket):
                raise Exception(f"Bucket {bucket} does not exist and could not be created")

            # Fetch the object from MinIO
            response = self.minio_client.get_object(bucket, path)
            try:
                # Read the content and decode it as UTF-8
                json_data = response.read().decode('utf-8')
            finally:
                response.close()
                response.release_conn()

            # Parse the JSON string into a Python dictionary
            data = json.loads(json_data)
            if not isinstance(data, dict):
                raise ValueError("Fetched JSON data is not a dictionary")

            logging.info(f"Successfully fetched JSON file from {bucket}/{path}")
            return data

        except S3Error as e:
            logging.error(f"Error fetching JSON file from {bucket}/{path}: {e}")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON format in {bucket}/{path}: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error fetching JSON file from {bucket}/{path}: {e}")
            raise