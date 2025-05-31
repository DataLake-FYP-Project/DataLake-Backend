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

# Load environment variables
load_dotenv()


def process_video_data(spark, input_path, video_type):
    """Process video data and return processed DataFrame with output path"""
    minio_conn = MinIOConnector(spark)

    try:
        type_folder = "vehicle_detection" if video_type.lower() == "vehicle" else \
            "people_detection" if video_type.lower() == "people" else \
                "geolocation_detection"
        full_input_path = f"{type_folder}/{input_path}"

        raw_df = minio_conn.read_json(BUCKETS["raw"], full_input_path)

        if video_type.lower() == "vehicle":
            processed_df, processing_status = process_vehicle_json_data(raw_df)
            output_path = f"vehicle_detection/preprocessed_{os.path.splitext(os.path.basename(input_path))[0]}.json"
        elif video_type.lower() == "people":
            processed_df, processing_status = process_people_json_data(raw_df)
            output_path = f"people_detection/preprocessed_{os.path.splitext(os.path.basename(input_path))[0]}.json"
        else:  # geolocation
            processed_df, processing_status = process_geolocation_json_data(raw_df)
            output_path = f"geolocation_detection/preprocessed_{os.path.splitext(os.path.basename(input_path))[0]}.json"
        return processed_df, output_path, processing_status
    except Exception as e:
        logging.error(f"Data processing failed for {input_path}: {e}")
        raise


def write_output_json(spark, df, output_path, processing_status):
    try:
        clean_path = output_path.lstrip('/')
        minio_conn = MinIOConnector(spark)

        # Check if it's geolocation by file path
        is_geolocation = "geolocation_detection" in clean_path

        if is_geolocation:
            # Write wrapped JSON to "processed"
            minio_conn.write_wrapped_json(
                df=df,
                bucket=BUCKETS["processed"],
                path=clean_path,
                key="frame_detections"
            )
            # Also write wrapped JSON to "refine"
            refine_path = clean_path.replace("preprocessed_", "refine_")
            minio_conn.write_wrapped_json(
                df=df,
                bucket=BUCKETS["refine"],
                path=refine_path,
                key="frame_detections"
            )
        else:
            # Use standard write_json for people/vehicle
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
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    spark = create_spark_session()
    minio_conn = MinIOConnector(spark)
    processing_status = None

    if not minio_conn.ensure_bucket_exists(BUCKETS["raw"]):
        logging.error("Cannot proceed without required buckets")
        return processing_status

    print("\n")
    logging.info("Starting Basic Preprocessing")
    start_time = datetime.now(timezone.utc)
    try:
        # Get files using the MinIOConnector method
        if detection_type == "Vehicle":
            vehicle_file = minio_conn.get_json_file(BUCKETS["raw"], f"vehicle_detection/{filename}")
            if not vehicle_file:
                logging.warning(f"No {filename} file found in vehicle_detection folder")

            # Process vehicle files
            try:
                logging.info(f"Processing vehicle file: {vehicle_file}")
                vehicle_df, vehicle_path, processing_status = process_video_data(spark, vehicle_file, "vehicle")
                if not write_output_json(spark, vehicle_df, vehicle_path, processing_status):
                    logging.error(f"Failed to process vehicle file: {vehicle_file}")
            except Exception as e:
                logging.error(f"Error processing vehicle file {vehicle_file}: {e}")

        elif detection_type == "People":
            people_file = minio_conn.get_json_file(BUCKETS["raw"], f"people_detection/{filename}")
            if not people_file:
                logging.warning(f"No {filename} file found in raw bucket people detection folder")

            # Process people files
            try:
                logging.info(f"Processing people file: {people_file}")
                people_df, people_path, processing_status = process_video_data(spark, people_file, "people")
                if not write_output_json(spark, people_df, people_path, processing_status):
                    logging.error(f"Failed to process people file: {people_file}")
            except Exception as e:
                logging.error(f"Error processing people file {people_file}: {e}")

        elif detection_type == "Geolocation":
            geolocation_file = minio_conn.get_json_file(BUCKETS["raw"], f"geolocation_detection/{filename}")
            if not geolocation_file:
                logging.warning(f"No {filename} file found in geolocation_detection folder")

            try:
                logging.info(f"Processing geolocation file: {geolocation_file}")
                geolocation_df, geolocation_path, processing_status = process_video_data(spark, geolocation_file,
                                                                                         "geolocation")
                if not write_output_json(spark, geolocation_df, geolocation_path, processing_status):
                    logging.error(f"Failed to process geolocation file: {geolocation_file}")
                refine_output_path = geolocation_path.replace("preprocessed_", "refine_")
                minio_conn.write_json(geolocation_df, bucket="refine", path=refine_output_path, temp_bucket="processed")

            except Exception as e:
                logging.error(f"Error processing geolocation file {geolocation_file}: {e}")

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        if processing_status == -1:
            logging.info("No detections in raw json to process. Skipping further preprocessing\n")
            return processing_status
        elif processing_status == 1 and detection_type != "Geolocation":
            logging.info(f"Basic Processing completed in {duration:.2f} seconds")

            print("\n")
            logging.info("Starting Advanced Preprocessing ")
            try:
                processing_status = advanced_preprocessing(detection_type, filename)
            except Exception as e:
                logging.error(f"Error during advanced preprocessing: {e}")
                processing_status = None

            print("\n")
            logging.info("All processing stages completed ")
            return processing_status
        else:
            return processing_status
    except Exception as e:
        logging.error(f"Fatal error in processing pipeline: {e}")
        return processing_status
    finally:
        spark.stop()
        logging.info("Spark session stopped")
        print("\n")