import json
import sys
from flask import Flask, request, jsonify
import os
import sys
from pathlib import Path

# Add parent folder to Python's search path
sys.path.append(str(Path(__file__).parent.parent))

from Preprocess_Json_Data.main import spark_preprocessing
from processing_vehicle import convert_json_format, vehicle_upload_to_minio, vehicle_upload_to_elasticsearch
from processing_people import convert_people_json_format, people_upload_to_elasticsearch, people_upload_to_minio

app = Flask(__name__)


@app.route("/upload_2_vehicle", methods=["POST"])
def upload_vehicle_json():
    if "json_file" not in request.files:
        return jsonify({"error": "No JSON file uploaded"}), 400

    json_file = request.files["json_file"]
    if json_file.filename == "":
        return jsonify({"error": "No selected file"}), 400

    filename = json_file.filename
    video_name = filename.split('.')[0]
    print("video name", video_name)

    json_folder_vehicle = "Vehicle_Json_Folder"
    os.makedirs(json_folder_vehicle, exist_ok=True)

    json_path = os.path.join(json_folder_vehicle, filename)
    json_file.save(json_path)

    vehicle_upload_to_minio(json_path, video_name)

    spark_preprocessing(filename, "Vehicle")

    # Process JSON
    # processed_json = f"{video_name}_processed.json"
    # convert_json_format(json_path, processed_json)

    # Upload both original and processed
    # vehicle_upload_to_minio(video_name)
    # vehicle_upload_to_elasticsearch(processed_json)

    return jsonify({"message": "Vehicle file uploaded and processed successfully"}), 200


@app.route("/upload_2_people", methods=["POST"])
def upload_people_json():
    if "json_file" not in request.files:
        return jsonify({"error": "No JSON file uploaded"}), 400

    json_file = request.files["json_file"]
    if json_file.filename == "":
        return jsonify({"error": "No selected file"}), 400

    filename = json_file.filename
    video_name = filename.split('.')[0]

    json_folder_people = "People_Json_Folder"
    os.makedirs(json_folder_people, exist_ok=True)

    # Save uploaded file
    json_path = os.path.join(json_folder_people, filename)
    json_file.save(json_path)

    # 🔁 Convert original JSON into processed format
    # processed_json_path = os.path.join(json_folder_people, f"{video_name}_processed.json")
    # convert_people_json_format(json_path, processed_json_path)

    # Upload original (raw) file to MinIO and processed to Elasticsearch
    people_upload_to_minio(json_path, video_name)

    spark_preprocessing(filename, "People")

    # people_upload_to_elasticsearch(processed_json_path)

    return jsonify({"message": "People file uploaded and processed successfully"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8013, debug=True, use_reloader=False)
