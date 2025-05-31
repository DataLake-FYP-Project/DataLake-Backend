# from Preprocess_Json_Data.preprocessing.advanced_preprocessing import CombinedProcessor
from datetime import datetime, timezone
from Preprocess_Json_Data.preprocessing.advanced_preprocessing import advanced_preprocessing
from Preprocess_Json_Data.config.spark_config import create_spark_session
from Preprocess_Json_Data.config.minio_config import BUCKETS
from Preprocess_Json_Data.connectors.minio_connector import MinIOConnector
from Preprocess_Json_Data.preprocessing.basic_preprocessing_vehicle import process_vehicle_json_data
import logging
import os
from dotenv import load_dotenv
from Preprocess_Json_Data.preprocessing.basic_preprocessing_people import process_people_json_data
from Preprocess_Json_Data.preprocessing.basic_preprocessing_geolocation import process_geolocation_json_data
from Preprocess_Json_Data.safety.basic_preprocessing_safety import process_safety_json_data
from Preprocess_Json_Data.registry import DETECTION_REGISTRY

# Load environment variables
load_dotenv()


def process_video_data(spark, input_path, video_type):
    """Generic processor for all detection types"""
    video_type = video_type.lower()
    if video_type not in DETECTION_REGISTRY:
        raise ValueError(f"Unsupported detection type: {video_type}")

    minio_conn = MinIOConnector(spark)
    config = DETECTION_REGISTRY[video_type]

    full_input_path = f"{config['folder']}/{input_path}"
    raw_df = minio_conn.read_json(BUCKETS["raw"], full_input_path)

    processed_df, processing_status = config["processor"](raw_df)
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    output_path = f"{config['folder']}/preprocessed_{base_name}.json"

    return processed_df, output_path, processing_status


def write_output_json(spark, df, output_path, processing_status):
    try:
        clean_path = output_path.lstrip('/')
        video_type = clean_path.split('/')[0].replace('_detection', '')
        config = DETECTION_REGISTRY.get(video_type)

        minio_conn = MinIOConnector(spark)

        if config and config["wrapped"]:
            # Write wrapped JSON
            minio_conn.write_wrapped_json(df, BUCKETS["processed"], clean_path, key="frame_detections")

            refine_path = clean_path.replace("preprocessed_", "refine_")
            minio_conn.write_wrapped_json(df, BUCKETS["refine"], refine_path, key="frame_detections")
        else:
            minio_conn.write_json(df, BUCKETS["processed"], clean_path)

        if processing_status == 1:
            logging.info(f"Successfully wrote output to processed/{clean_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to write output to processed/{clean_path}: {e}")
        return False


def fetch_refined_file(spark, file_path: str, file_name: str, detection_type: str):
    """
    Fetch a specific refined JSON file from the refine bucket.

    Args:
        spark: Spark session instance
        file_path: Path to the file (e.g., 'vehicle_detection' or 'people_detection')
        file_name: Name of the file (e.g., 'refine_preprocessed_vehicle-counting1_2025-05-06_12-20-22.json')
        detection_type: Type of detection ('Vehicle' or 'People')

    Returns:
        Dict containing the parsed JSON data

    Raises:
        Exception: If the file cannot be fetched or parsed
    """
    try:
        minio_conn = MinIOConnector(spark)

        # Construct the full file path
        # type_folder = "vehicle_detection" if detection_type.lower() == "vehicle" else "people_detection"
        full_path = f"{file_path}/{file_name}" if file_path else file_name

        # Ensure the refine bucket exists
        if not minio_conn.ensure_bucket_exists(BUCKETS["refine"]):
            raise Exception(f"Refine bucket {BUCKETS['refine']} does not exist and could not be created")

        # Fetch the JSON file
        logging.info(f"Fetching refined file: {full_path} from bucket {BUCKETS['refine']}")
        data = minio_conn.fetch_json(BUCKETS["refine"], full_path)

        return data

    except Exception as e:
        logging.error(f"Failed to fetch refined file {full_path} from {BUCKETS['refine']}: {e}")
        raise


def spark_preprocessing(filename, detection_type):
    detection_type = detection_type.lower()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    spark = create_spark_session()
    minio_conn = MinIOConnector(spark)
    processing_status = None

    if detection_type not in DETECTION_REGISTRY:
        logging.error(f"Unknown detection type: {detection_type}")
        return None

    config = DETECTION_REGISTRY[detection_type]

    if not minio_conn.ensure_bucket_exists(BUCKETS["raw"]):
        logging.error("Raw bucket not found")
        return None

    try:
        raw_file_path = f"{config['folder']}/{filename}"
        file = minio_conn.get_json_file(BUCKETS["raw"], raw_file_path)

        if not file:
            logging.warning(f"No {filename} file found in {config['folder']} folder")
            return None

        logging.info(f"Processing file: {file}")
        df, output_path, processing_status = process_video_data(spark, file, detection_type)

        if not write_output_json(spark, df, output_path, processing_status):
            logging.error(f"Failed to write output for: {file}")
            return None

        if processing_status == -1:
            logging.info("No detections to process")
            return -1

        if processing_status == 1 and not config.get("wrapped"):
            logging.info("Starting Advanced Preprocessing")
            try:
                processing_status = advanced_preprocessing(detection_type, filename)
            except Exception as e:
                logging.error(f"Advanced preprocessing error: {e}")
                return None

        logging.info("Processing completed successfully")
        return processing_status
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        return None
    finally:
        spark.stop()
