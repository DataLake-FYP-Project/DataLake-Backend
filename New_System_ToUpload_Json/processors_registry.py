# processors_registry.py

from process_scripts.advanced_preprocessing_vehicle import VehicleProcessor
from process_scripts.basic_preprocessing_vehicle import process_vehicle_json_data

# Example registry mapping type → processors, process funcs, buckets
PROCESSOR_REGISTRY = {
    "vehicle": {
        "processor_class": VehicleProcessor,
        "process_func": process_vehicle_json_data,
        "raw_bucket": "raw",
        "processed_bucket": "processed",
        "refine_bucket": "refine",
    },
    # "people": {
    #     "processor_class": PeopleProcessor,
    #     "process_func": process_people_json_data,
    #     "raw_bucket": "raw_people",
    #     "processed_bucket": "processed_people",
    #     "refine_bucket": "refine_people",
    # },
    # Add more as needed, e.g. "animal", "safety"...
}
