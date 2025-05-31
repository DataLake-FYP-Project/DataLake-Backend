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
from processing_safety import safety_upload_to_minio
from processing_people import convert_people_json_format, people_upload_to_elasticsearch, people_upload_to_minio
from processing_geolocation import geolocation_upload_to_elasticsearch, geolocation_upload_to_minio

app = Flask(__name__)


def handle_upload(json_file, folder_name, detection_type, upload_to_minio, upload_to_elasticsearch):
    filename = json_file.filename
    video_name = filename.split('.')[0]

    # Save file locally
    os.makedirs(folder_name, exist_ok=True)
    json_path = os.path.join(folder_name, filename)
    json_file.save(json_path)
    logging.info(f"Saved file to local path: {json_path}")

    upload_to_minio(json_path)
    logging.info(f"Uploaded file to MinIO raw bucket")

    # Preprocessing with Spark
    processing_status = spark_preprocessing(filename, detection_type.lower())
    logging.info("Completed Spark preprocessing")

    if processing_status != 1:
        logging.info("Nothing to query/dashboard. Stop calling Elasticsearch")
        return {"message": "Nothing to query/dashboard. Stop calling Elasticsearch"}, 200

    # Fetch and index
    spark = create_spark_session()
    minio_conn = MinIOConnector(spark)
    temp_file_path = None
    try:
        refine_bucket = "refine"
        base_name = video_name.split("preprocessed_")[1] if "preprocessed_" in video_name else video_name
        prefix = f"{detection_type.lower()}_detection/refine_{base_name}"
        logging.info(f"Listing refined files with prefix: {prefix} in bucket: {refine_bucket}")

        refined_files = minio_conn.list_json_files(refine_bucket, prefix)
        if not refined_files:
            logging.error(f"No refined files found for {video_name}")
            return {"error": f"No refined files found for {video_name}"}, 404

        objects = list(minio_conn.minio_client.list_objects(refine_bucket, prefix=prefix, recursive=True))
        if not objects:
            logging.error(f"No objects found with prefix {prefix}")
            return {"error": f"No objects found with prefix {prefix}"}, 404

        latest_file = max(objects, key=lambda x: x.last_modified)
        refined_file_name = latest_file.object_name.split('/')[-1]
        logging.info(f"Selected latest refined file: {refined_file_name}")

        refined_data = fetch_refined_file(
            spark,
            file_path=f"{detection_type.lower()}_detection",
            file_name=refined_file_name,
            detection_type=detection_type
        )

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
            json.dump(refined_data, temp_file, indent=4)
            temp_file_path = temp_file.name
        logging.info(f"Saved refined data to temporary file: {temp_file_path}")

        upload_to_elasticsearch(temp_file_path)
        logging.info("Successfully uploaded to Elasticsearch")

    except Exception as e:
        logging.error(f"Error in fetch/upload process: {str(e)}", exc_info=True)
        return {"error": f"Failed to fetch or upload refined JSON: {str(e)}"}, 500
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        spark.stop()

    return {"message": f"{detection_type} file uploaded, processed, and indexed successfully"}, 200



@app.route("/upload_2_vehicle", methods=["POST"])
def upload_vehicle_json():
    if "json_file" not in request.files:
        return jsonify({"error": "No JSON file uploaded"}), 400
    return jsonify(*handle_upload(
        json_file=request.files["json_file"],
        folder_name="Vehicle_Json_Folder",
        detection_type="Vehicle",
        upload_to_minio=vehicle_upload_to_minio,
        upload_to_elasticsearch=vehicle_upload_to_elasticsearch
    ))

@app.route("/upload_2_geolocation", methods=["POST"])
def upload_geolocation_json():
    if "json_file" not in request.files:
        return jsonify({"error": "No JSON file uploaded"}), 400
    return jsonify(*handle_upload(
        json_file=request.files["json_file"],
        folder_name="Geolocation_Json_Folder",
        detection_type="Geolocation",
        upload_to_minio=geolocation_upload_to_minio,
        upload_to_elasticsearch=geolocation_upload_to_elasticsearch
    ))

@app.route("/upload_2_people", methods=["POST"])
def upload_people_json():
    if "json_file" not in request.files:
        return jsonify({"error": "No JSON file uploaded"}), 400
    return jsonify(*handle_upload(
        json_file=request.files["json_file"],
        folder_name="People_Json_Folder",
        detection_type="People",
        upload_to_minio=people_upload_to_minio,
        upload_to_elasticsearch=people_upload_to_elasticsearch
    ))

@app.route("/upload_2_safety", methods=["POST"])
def upload_safety_json():
    if "json_file" not in request.files:
        return jsonify({"error": "No JSON file uploaded"}), 400
    return jsonify(*handle_upload(
        json_file=request.files["json_file"],
        folder_name="Safety_Json_Folder",
        detection_type="Safety",
        upload_to_minio=safety_upload_to_minio,
        upload_to_elasticsearch=people_upload_to_elasticsearch
    ))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8013, debug=True, use_reloader=False)