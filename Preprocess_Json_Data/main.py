from pyspark.sql import SparkSession
from config.spark_config import create_spark_session
from config.minio_config import BUCKETS, MINIO_CONFIG
from connectors.minio_connector import MinIOConnector
from preprocessing.frame_data import process_frame_data
from preprocessing.tracking_data import process_tracking_data
import logging
import os
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

# Load environment variables
load_dotenv()

def ensure_buckets_exist():
    """Ensure required MinIO buckets exist with proper folder structure"""
    try:
        minio_client = Minio(
            MINIO_CONFIG["endpoint"].replace("http://", ""),
            access_key=MINIO_CONFIG["access_key"],
            secret_key=MINIO_CONFIG["secret_key"],
            secure=MINIO_CONFIG["secure"]
        )
        
        # Create raw bucket with folders if it doesn't exist
        if not minio_client.bucket_exists(BUCKETS["raw"]):
            minio_client.make_bucket(BUCKETS["raw"])
            logging.info(f"Created bucket: {BUCKETS['raw']}")
        
        # Create processed bucket with folders if it doesn't exist
        if not minio_client.bucket_exists(BUCKETS["processed"]):
            minio_client.make_bucket(BUCKETS["processed"])
            logging.info(f"Created bucket: {BUCKETS['processed']}")
        
        return True
    except S3Error as e:
        logging.error(f"MinIO bucket creation error: {e}")
        return False
    except Exception as e:
        logging.error(f"Error verifying buckets: {e}")
        return False

def process_video_data(spark, input_path, video_type):
    """Process video data and return processed DataFrame with output path"""
    minio_conn = MinIOConnector(spark)
    
    try:
        # Construct full input path based on type
        type_folder = "vehicle_detection" if video_type.lower() == "vehicle" else "people_detection"
        full_input_path = f"{type_folder}/{input_path}"
        
        # Read JSON data from MinIO raw bucket
        raw_df = minio_conn.read_json(BUCKETS["raw"], full_input_path)
        
        # Process based on video type
        if video_type.lower() == "vehicle":
            processed_df = process_frame_data(raw_df, {
                "confidence_threshold": 0.7,
                "default_values": {
                    "confidence": 0.5,
                    "tracker_id": -1,
                    "class_id": -1
                }
            })
            # Changed output path format
            output_path = f"vehicle_detection/{os.path.splitext(os.path.basename(input_path))[0]}.json"
        else:
            processed_df = process_tracking_data(raw_df, {
                "timestamp_format": "yyyy-MM-dd HH:mm:ss",
                "default_values": {
                    "age": -1,
                    "confidence": 0.5,
                    "gender": "Unknown"
                }
            })
            # Changed output path format
            output_path = f"people_detection/{os.path.splitext(os.path.basename(input_path))[0]}.json"
        
        return processed_df, output_path
    except Exception as e:
        logging.error(f"Data processing failed for {input_path}: {e}")
        raise

def write_output_json(spark, df, output_path):
    """Write processed data to MinIO as JSON"""
    try:
        # Ensure we're writing to the processed bucket with clean path
        clean_path = output_path.lstrip('/')  # Remove any leading slashes
        
        MinIOConnector(spark).write_json(
            df,
            BUCKETS["processed"],  # This ensures it goes to the processed bucket
            clean_path
        )
        logging.info(f"Successfully wrote output to processed/{clean_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to write output to processed/{clean_path}: {e}")
        return False

def main():
    # Initialize logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Verify MinIO buckets exist
    if not ensure_buckets_exist():
        logging.error("Cannot proceed without required buckets")
        return

    # Initialize Spark
    spark = create_spark_session()
    
    try:
        # Example files - replace with your actual file names
        vehicle_files = ["vehicle_video_frame_data.json"]
        people_files = ["people_video_frame_data.json"]
        
        # Process and save vehicle data
        for vehicle_file in vehicle_files:
            vehicle_df, vehicle_path = process_video_data(
                spark,
                vehicle_file,
                "vehicle"
            )
            if not write_output_json(spark, vehicle_df, vehicle_path):
                logging.error(f"Failed to process vehicle file: {vehicle_file}")

        # Process and save people data
        for people_file in people_files:
            people_df, people_path = process_video_data(
                spark,
                people_file,
                "people"
            )
            if not write_output_json(spark, people_df, people_path):
                logging.error(f"Failed to process people file: {people_file}")

        logging.info("Processing completed")
    except Exception as e:
        logging.error(f"Fatal error in processing pipeline: {e}")
    finally:
        spark.stop()
        logging.info("Spark session stopped")

if __name__ == "__main__":
    main()