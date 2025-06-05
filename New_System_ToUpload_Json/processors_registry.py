
# Example registry mapping type → processors, process funcs, buckets
from process_scripts.safety.advanced_preprocessing_safety import SafetyProcessor
from process_scripts.safety.basic_preprocessing_safety import process_safety_json_data
from process_scripts.vehicle.advanced_preprocessing_vehicle import VehicleProcessor
from process_scripts.vehicle.basic_preprocessing_vehicle import process_vehicle_json_data


PROCESSOR_REGISTRY = {
    "vehicle": {
        "processor_class": VehicleProcessor,
        "process_func": process_vehicle_json_data,
        "folder_prefix": "vehicle_detection"
    },
    "safety": {
        "processor_class": SafetyProcessor,
        "process_func": process_safety_json_data,
        "folder_prefix": "safety_detections"
    },

    
    # Add more as needed, e.g. "animal", "safety"...
}
