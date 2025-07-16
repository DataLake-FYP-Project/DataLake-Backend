
# Example registry mapping type → processors, process funcs, buckets
from process_scripts.retail.advanced_preprocessing_retail import RetailProcessor
from process_scripts.retail.basic_preprocessing_retail import process_retail_json_data
from process_scripts.safety.advanced_preprocessing_safety import SafetyProcessor
from process_scripts.safety.basic_preprocessing_safety import process_safety_json_data
from process_scripts.school.advanced_preprocessing_school import SchoolProcessor
from process_scripts.school.basic_preprocessing_school import process_school_json_data
from process_scripts.vehicle.advanced_preprocessing_vehicle import VehicleProcessor
from process_scripts.vehicle.basic_preprocessing_vehicle import process_vehicle_json_data


PROCESSOR_REGISTRY = {
    "vehicle": {
        "processor_class": VehicleProcessor,
        "process_func": process_vehicle_json_data,
        "folder_prefix": "vehicle_detection",
        "ELK_index":"datalake-vehicle-data"
    },
    "safety": {
        "processor_class": SafetyProcessor,
        "process_func": process_safety_json_data,
        "folder_prefix": "safety_detections",
        "ELK_index":"datalake-safety-data"
    },
    "school": {
        "processor_class": SchoolProcessor,
        "process_func": process_school_json_data,
        "folder_prefix": "school_detections",
        "ELK_index":"datalake-school-data"
    },
    "retail": {
        "processor_class": RetailProcessor,
        "process_func": process_retail_json_data,
        "folder_prefix": "retail_detections",
        "ELK_index":"datalake-retail-data"
    },

    
    # Add more as needed, e.g. "animal", "parking"...
}
