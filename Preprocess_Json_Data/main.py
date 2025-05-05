# from Preprocess_Json_Data.preprocessing.advanced_preprocessing import CombinedProcessor
from Preprocess_Json_Data.preprocessing.advanced_preprocessing import advanced_preprocessing
from Preprocess_Json_Data.config.spark_config import create_spark_session
from Preprocess_Json_Data.config.minio_config import BUCKETS
from Preprocess_Json_Data.connectors.minio_connector import MinIOConnector
from Preprocess_Json_Data.preprocessing.frame_data import process_frame_data
import logging
import os
from dotenv import load_dotenv
from Preprocess_Json_Data.preprocessing.frame_data_people_detection import process_people_json_data

# Load environment variables
load_dotenv()


def process_video_data(spark, input_path, video_type):
    """Process video data and return processed DataFrame with output path"""
    minio_conn = MinIOConnector(spark)

    try:
        type_folder = "vehicle_detection" if video_type.lower() == "vehicle" else "people_detection"
        full_input_path = f"{type_folder}/{input_path}"

        raw_df = minio_conn.read_json(BUCKETS["raw"], full_input_path)

        if video_type.lower() == "vehicle":
            processed_df = process_frame_data(raw_df)
            output_path = f"vehicle_detection/preprocessed_{os.path.splitext(os.path.basename(input_path))[0]}.json"
        else:
            processed_df = process_people_json_data(raw_df)
            output_path = f"people_detection/preprocessed_{os.path.splitext(os.path.basename(input_path))[0]}.json"

        return processed_df, output_path
    except Exception as e:
        logging.error(f"Data processing failed for {input_path}: {e}")
        raise


def write_output_json(spark, df, output_path):
    try:
        clean_path = output_path.lstrip('/')
        MinIOConnector(spark).write_json(df, BUCKETS["processed"], clean_path)
        logging.info(f"Successfully wrote output to processed/{clean_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to write output to processed/{clean_path}: {e}")
        return False


def spark_preprocessing(filename, detection_type):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    spark = create_spark_session()
    minio_conn = MinIOConnector(spark)

    if not minio_conn.ensure_bucket_exists(BUCKETS["raw"]):
        logging.error("Cannot proceed without required buckets")
        return

    try:
        # Get files using the MinIOConnector method
        if detection_type == "Vehicle":
            vehicle_files = minio_conn.list_json_files(BUCKETS["raw"], f"vehicle_detection/{filename}")
            if not vehicle_files:
                logging.warning("No vehicle detection files found in raw bucket")

            print("\n=== Starting Basic Preprocessing=== ")

            # Process vehicle files
            for vehicle_file in vehicle_files:
                try:
                    logging.info(f"Processing vehicle file: {vehicle_file}")
                    vehicle_df, vehicle_path = process_video_data(spark, vehicle_file, "vehicle")
                    if not write_output_json(spark, vehicle_df, vehicle_path):
                        logging.error(f"Failed to process vehicle file: {vehicle_file}")
                except Exception as e:
                    logging.error(f"Error processing vehicle file {vehicle_file}: {e}")

        elif detection_type == "People":
            people_files = minio_conn.list_json_files(BUCKETS["raw"], f"people_detection/")
            if not people_files:
                logging.warning("No people detection files found in raw bucket")

            print("\n=== Starting Basic Preprocessing=== ")

            # Process people files
            for people_file in people_files:
                try:
                    logging.info(f"Processing people file: {people_file}")
                    people_df, people_path = process_video_data(spark, people_file, "people")
                    if not write_output_json(spark, people_df, people_path):
                        logging.error(f"Failed to process people file: {people_file}")
                except Exception as e:
                    logging.error(f"Error processing people file {people_file}: {e}")

        logging.info("Basic Processing completed")

        print("\n=== Starting Advanced Preprocessing=== ")
        try:
            advanced_preprocessing(detection_type)
        except Exception as e:
            logging.error(f"Error during advanced preprocessing: {e}")

        print("\n=== All processing stages completed=== ")
    except Exception as e:
        logging.error(f"Fatal error in processing pipeline: {e}")
    finally:
        spark.stop()
        logging.info("Spark session stopped")
