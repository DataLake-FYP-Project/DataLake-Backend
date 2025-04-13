from flask import Flask, request, jsonify
import os
from processing_vehicle import convert_json_format, vehicle_upload_to_minio, vehicle_upload_to_elasticsearch
from processing_people import people_upload_to_elasticsearch, people_upload_to_minio


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
    print("video name" ,video_name)

    json_folder_vehicle = "Vehicle_Json_Folder"
    os.makedirs(json_folder_vehicle, exist_ok=True)

    json_path = os.path.join(json_folder_vehicle, filename)
    json_file.save(json_path)

    # Process JSON
    processed_json = f"{video_name}_processed.json"
    convert_json_format(json_path, processed_json)

    # Upload both original and processed
    vehicle_upload_to_minio(json_path, video_name)
    vehicle_upload_to_minio(processed_json, video_name)
    vehicle_upload_to_elasticsearch(processed_json)

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

    json_path = os.path.join(json_folder_people, filename)
    json_file.save(json_path)

    # Upload original file only (twice to MinIO as per your earlier logic)
    people_upload_to_minio(json_path, video_name)
    people_upload_to_elasticsearch(json_path)

    return jsonify({"message": "People file uploaded successfully"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8013, debug=True, use_reloader=False)
