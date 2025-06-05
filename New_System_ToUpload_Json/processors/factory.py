from processors.vehicle_processor import VehicleDataProcessor
# from processors.people_processor import PeopleDataProcessor
# Add other imports as you scale

def get_processor(data_type: str, spark):
    processors = {
        "vehicle": VehicleDataProcessor,
        # "people": PeopleDataProcessor,
    }
    if data_type not in processors:
        raise ValueError(f"Unsupported data type: {data_type}")
    return processors[data_type](spark)