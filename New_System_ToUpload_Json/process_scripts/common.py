from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, lit, coalesce, when, trim, current_timestamp, md5
from pyspark.sql.types import StructType, StringType
import logging
from minio import Minio
from io import BytesIO


def validate_schema(df: DataFrame, expected_schema: StructType) -> DataFrame:
    """Validate DataFrame schema against expected schema with type enforcement"""
    for field in expected_schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, lit(None).cast(field.dataType))
        else:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    return df

def clean_string_columns(df: DataFrame) -> DataFrame:
    """Trim whitespace from string columns and handle nulls"""
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col_name in string_cols:
        df = df.withColumn(
            col_name, 
            when(col(col_name).isNotNull(), trim(col(col_name))))
    return df

def handle_null_values(df: DataFrame, default_values: dict) -> DataFrame:
    """Replace null values with defaults with proper type handling"""
    for col_name, default_val in default_values.items():
        if col_name in df.columns:
            df = df.withColumn(
                col_name, 
                coalesce(
                    col(col_name), 
                    lit(default_val).cast(df.schema[col_name].dataType)
                )
            )
    return df

def convert_timestamps(df: DataFrame, timestamp_cols: list, format: str = "yyyy-MM-dd HH:mm:ss") -> DataFrame:
    """Convert string timestamps to timestamp type with error handling"""
    for col_name in timestamp_cols:
        if col_name in df.columns:
            df = df.withColumn(
                col_name, 
                to_timestamp(col(col_name), format)
            )
    return df

def upload_to_minio(bucket_name, object_name, data_str, minio_config):
    client = Minio(
        minio_config["endpoint"],
        access_key=minio_config["access_key"],
        secret_key=minio_config["secret_key"],
        secure=False
    )

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    data_bytes = BytesIO(data_str.encode('utf-8'))
    client.put_object(bucket_name, object_name, data_bytes, length=len(data_str))


def save_processed_json_to_minio(processed_df, minio_connector, bucket_name, upload_filename, wrapped=False):
    try:
        clean_path = upload_filename.lstrip('/')
        if wrapped:
            minio_connector.write_wrapped_json(processed_df, bucket_name, clean_path, key="frame_detections")
        else:
            minio_connector.write_json(processed_df, bucket_name, clean_path)
        logging.info(f"Successfully uploaded processed JSON to {bucket_name}/{clean_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to upload processed JSON to {bucket_name}/{upload_filename}: {e}")
        return False

    
def save_refined_json_to_minio(refined_df, minio_connector, bucket_name, upload_filename, wrapped=True):
    try:
        clean_path = upload_filename.lstrip('/')
        minio_connector.write_wrapped_json(refined_df, bucket_name, clean_path, key="frame_detections")
        logging.info(f"Successfully uploaded refined JSON to {bucket_name}/{clean_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to upload refined JSON to {bucket_name}/{upload_filename}: {e}")
        return False
    
def get_common_output_structure(source_file):
    """Common output structure for all types"""
    return {
        "source_file": source_file,
        "processing_date": datetime.now(timezone.utc).isoformat(),
        "processing_version": "1.0"
    }