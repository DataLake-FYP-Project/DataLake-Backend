from Preprocess_Json_Data.preprocessing.basic_preprocessing_animal import process_animal_json_data
from Preprocess_Json_Data.preprocessing.basic_preprocessing_geolocation import process_geolocation_json_data
from Preprocess_Json_Data.preprocessing.basic_preprocessing_people import process_people_json_data
from Preprocess_Json_Data.preprocessing.basic_preprocessing_safety import process_safety_json_data
from Preprocess_Json_Data.preprocessing.basic_preprocessing_vehicle import process_vehicle_json_data
from Preprocess_Json_Data.preprocessing.basic_preprocessing_pose import process_pose_json_data


DETECTION_REGISTRY = {
    "vehicle": {
        "folder": "vehicle_detection",
        "processor": process_vehicle_json_data,
        "wrapped": False
    },
    "people": {
        "folder": "people_detection",
        "processor": process_people_json_data,
        "wrapped": False
    },
    "geolocation": {
        "folder": "geolocation_detection",
        "processor": process_geolocation_json_data,
        "wrapped": True
    },
    "safety": {
        "folder": "safety_detection",
        "processor": process_safety_json_data,
        "wrapped": False
    },
    "pose": {
        "folder": "pose_detection",
        "processor": process_pose_json_data,
        "wrapped": True
    },
    "animal": {
        "folder": "animal_detection",
        "processor": process_animal_json_data,
        "wrapped": True
    }
}
