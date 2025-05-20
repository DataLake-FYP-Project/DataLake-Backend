import json
import sys
from flask import Flask, request, jsonify
import os
from pathlib import Path
import logging
import tempfile

# Configure logging with more detail
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Add parent folder to Python's search path
sys.path.append(str(Path(__file__).parent.parent))

from Preprocess_Json_Data.config.spark_config import create_spark_session
from Preprocess_Json_Data.connectors.minio_connector import MinIOConnector
from Preprocess_Json_Data.main import spark_preprocessing, fetch_refined_file
from processing_vehicle import convert_json_format, vehicle_upload_to_minio, vehicle_upload_to_elasticsearch
from processing_people import convert_people_json_format, people_upload_to_elasticsearch, people_upload_to_minio

app = Flask(__name__)

@app.route("/upload_2_vehicle", methods=["POST"])
def upload_vehicle_json():
    if "json_file" not in request.files:
        logging.error("No JSON file uploaded in request")
        return jsonify({"error": "No JSON file uploaded"}), 400

    json_file = request.files["json_file"]
    if json_file.filename == "":
        logging.error("No selected file in request")
        return jsonify({"error": "No selected file"}), 400

    filename = json_file.filename
    video_name = filename.split('.')[0]
    logging.info(f"Received file: {filename}, video name: {video_name}")

    json_folder_vehicle = "Vehicle_Json_Folder"
    os.makedirs(json_folder_vehicle, exist_ok=True)

    json_path = os.path.join(json_folder_vehicle, filename)
    json_file.save(json_path)
    logging.info(f"Saved file to local path: {json_path}")

    vehicle_upload_to_minio(json_path)
    logging.info(f"Uploaded file to MinIO raw bucket")

    # Process the file using Spark (this will create the refined JSON in the refine bucket)
    processing_status=spark_preprocessing(filename, "Vehicle")
    logging.info("Completed Spark preprocessing")

    if processing_status==1:
        # Fetch the most recent refined JSON from the refine bucket
        spark = create_spark_session()
        minio_conn = MinIOConnector(spark)
        temp_file_path = None
        try:
            # Construct the prefix for refined files
            refine_bucket = "refine"  # Adjust if BUCKETS["refine"] is different
            if "preprocessed_" in video_name:
                base_name = video_name.split("preprocessed_")[1]
            else:
                base_name = video_name
            prefix = f"vehicle_detection/refine_{base_name}"
            logging.info(f"Listing refined files with prefix: {prefix} in bucket: {refine_bucket}")

            # List refined files
            refined_files = minio_conn.list_json_files(refine_bucket, prefix)
            logging.info(f"Found refined files: {refined_files}")

            if not refined_files:
                logging.error(f"No refined files found for {video_name} in {refine_bucket}")
                return jsonify({"error": f"No refined files found for {video_name} in {refine_bucket}"}), 404

            # Sort by last modified time
            logging.info("Fetching objects to determine the latest file")
            objects = list(minio_conn.minio_client.list_objects(refine_bucket, prefix=prefix, recursive=True))
            logging.info(f"Objects found: {[obj.object_name for obj in objects]}")
            if not objects:
                logging.error(f"No objects found with prefix {prefix} in {refine_bucket}")
                return jsonify({"error": f"No objects found with prefix {prefix} in {refine_bucket}"}), 404

            latest_file = max(objects, key=lambda x: x.last_modified)
            refined_file_name = latest_file.object_name.split('/')[-1]
            logging.info(f"Selected latest refined file: {refined_file_name}")

            # Fetch the refined JSON
            logging.info(f"Fetching refined JSON: {refined_file_name}")
            refined_data = fetch_refined_file(
                spark,
                file_path="vehicle_detection",
                file_name=refined_file_name,
                detection_type="Vehicle"
            )
            logging.info("Successfully fetched refined JSON")

            # Save the refined data to a temporary file
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
                json.dump(refined_data, temp_file, indent=4)
                temp_file_path = temp_file.name
            logging.info(f"Saved refined data to temporary file: {temp_file_path}")

            # Upload the refined JSON to Elasticsearch
            logging.info("Uploading refined JSON to Elasticsearch")
            try:
                vehicle_upload_to_elasticsearch(temp_file_path)
                logging.info("Successfully uploaded to Elasticsearch")
            except Exception as e:
                logging.error(f"Error uploading to Elasticsearch: {str(e)}", exc_info=True)
                raise  # Re-raise the exception to be caught by the outer try-except

        except Exception as e:
            logging.error(f"Error in fetch/upload process: {str(e)}", exc_info=True)
            return jsonify({"error": f"Failed to fetch or upload refined JSON to Elasticsearch: {str(e)}"}), 500
        finally:
            # Clean up the temporary file if it was created
            if temp_file_path and os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
                logging.info(f"Deleted temporary file: {temp_file_path}")
            spark.stop()
            logging.info("Spark session stopped after fetch/upload")

        return jsonify({"message": "Vehicle file uploaded, processed, and indexed successfully"}), 200
    else:
        logging.info("Nothing to query/dashboard. Stop calling elastic search")
        return jsonify({"message": "Nothing to query/dashboard. Stop calling elastic search"}), 200

@app.route("/upload_2_people", methods=["POST"])
def upload_people_json():
    if "json_file" not in request.files:
        logging.error("No JSON file uploaded in request")
        return jsonify({"error": "No JSON file uploaded"}), 400

    json_file = request.files["json_file"]
    if json_file.filename == "":
        logging.error("No selected file in request")
        return jsonify({"error": "No selected file"}), 400

    filename = json_file.filename
    video_name = filename.split('.')[0]
    logging.info(f"Received file: {filename}, video name: {video_name}")

    json_folder_people = "People_Json_Folder"
    os.makedirs(json_folder_people, exist_ok=True)

    # Save uploaded file
    json_path = os.path.join(json_folder_people, filename)
    json_file.save(json_path)
    logging.info(f"Saved file to local path: {json_path}")

    # Upload original (raw) file to MinIO
    people_upload_to_minio(json_path)
    logging.info(f"Uploaded file to MinIO raw bucket")

    # Process the file using Spark (this will create the refined JSON in the refine bucket)
    processing_status=spark_preprocessing(filename, "People")
    logging.info("Completed Spark preprocessing")

    if processing_status==1:
        # Fetch the most recent refined JSON from the refine bucket
        spark = create_spark_session()
        minio_conn = MinIOConnector(spark)
        temp_file_path = None
        try:
            # Construct the prefix for refined files
            refine_bucket = "refine"  # Adjust if BUCKETS["refine"] is different
            if "preprocessed_" in video_name:
                base_name = video_name.split("preprocessed_")[1]
            else:
                base_name = video_name
            prefix = f"people_detection/refine_{base_name}"
            logging.info(f"Listing refined files with prefix: {prefix} in bucket: {refine_bucket}")

            # List refined files
            refined_files = minio_conn.list_json_files(refine_bucket, prefix)
            logging.info(f"Found refined files: {refined_files}")

            if not refined_files:
                logging.error(f"No refined files found for {video_name} in {refine_bucket}")
                return jsonify({"error": f"No refined files found for {video_name} in {refine_bucket}"}), 404

            # Sort by last modified time
            logging.info("Fetching objects to determine the latest file")
            objects = list(minio_conn.minio_client.list_objects(refine_bucket, prefix=prefix, recursive=True))
            logging.info(f"Objects found: {[obj.object_name for obj in objects]}")
            if not objects:
                logging.error(f"No objects found with prefix {prefix} in {refine_bucket}")
                return jsonify({"error": f"No objects found with prefix {prefix} in {refine_bucket}"}), 404

            latest_file = max(objects, key=lambda x: x.last_modified)
            refined_file_name = latest_file.object_name.split('/')[-1]
            logging.info(f"Selected latest refined file: {refined_file_name}")

            # Fetch the refined JSON
            logging.info(f"Fetching refined JSON: {refined_file_name}")
            refined_data = fetch_refined_file(
                spark,
                file_path="people_detection",
                file_name=refined_file_name,
                detection_type="People"
            )
            logging.info("Successfully fetched refined JSON")

            # Save the refined data to a temporary file
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
                json.dump(refined_data, temp_file, indent=4)
                temp_file_path = temp_file.name
            logging.info(f"Saved refined data to temporary file: {temp_file_path}")

            # Upload the refined JSON to Elasticsearch
            logging.info("Uploading refined JSON to Elasticsearch")
            try:
                people_upload_to_elasticsearch(temp_file_path)
                logging.info("Successfully uploaded to Elasticsearch")
            except Exception as e:
                logging.error(f"Error uploading to Elasticsearch: {str(e)}", exc_info=True)
                raise  # Re-raise the exception to be caught by the outer try-except

        except Exception as e:
            logging.error(f"Error in fetch/upload process: {str(e)}", exc_info=True)
            return jsonify({"error": f"Failed to fetch or upload refined JSON to Elasticsearch: {str(e)}"}), 500
        finally:
            # Clean up the temporary file if it was created
            if temp_file_path and os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
                logging.info(f"Deleted temporary file: {temp_file_path}")
            spark.stop()
            logging.info("Spark session stopped after fetch/upload")

        return jsonify({"message": "People file uploaded, processed, and indexed successfully"}), 200
    else:
        logging.info("Nothing to query/dashboard. Stop calling elastic search")
        return jsonify({"message": "Nothing to query/dashboard. Stop calling elastic search"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8013, debug=True, use_reloader=False)