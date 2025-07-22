from Preprocess_Json_Data.preprocessing import basic_processing_common
from Preprocess_Json_Data.preprocessing.basic_preprocessing_animal import process_animal_json_data
from Preprocess_Json_Data.preprocessing.basic_preprocessing_geolocation import process_geolocation_json_data
from Preprocess_Json_Data.preprocessing.basic_preprocessing_parkingLot import process_parking_json_data
from Preprocess_Json_Data.preprocessing.basic_preprocessing_people import process_people_json_data
from Preprocess_Json_Data.preprocessing.basic_preprocessing_safety import process_safety_json_data
from Preprocess_Json_Data.preprocessing.basic_preprocessing_vehicle import process_vehicle_json_data
from Preprocess_Json_Data.preprocessing.basic_preprocessing_pose import process_pose_json_data
from Preprocess_Json_Data.preprocessing.basic_processing_common import process_common_json_data

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
        "parking": {
        "folder": "parkingLot_detection",
        "processor": process_parking_json_data,
        "wrapped": False
    },
    "animal": {
        "folder": "animal_detection",
        "processor": process_animal_json_data,
        "wrapped": True
    },
    "common": {
        "folder": "common_detection",
        "processor": process_common_json_data,
        "wrapped": True
    }

}
