import json
from datetime import datetime
from statistics import mean
import sys
from typing import Dict, Any
from pathlib import Path

from dotenv import load_dotenv

# Add project root to Python path
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

from Preprocess_Json_Data.config.minio_config import BUCKETS
from minio_connector import MinIOConnector

class PeopleDataSplitter:
    def __init__(self, spark):
        self.spark = spark
        self.minio_connector = MinIOConnector(spark)
        self.source_file = "people_detection/refine_people_final_2025-05-20_12-54-03.json"

    def process(self):
        try:
            print(f"Processing {self.source_file} from {BUCKETS['refine']}")

            original_data = self._get_original_data()
            feature_files = self._transform_data(original_data)
            self._upload_files(feature_files)

            print("Processing completed successfully")
            return True

        except Exception as e:
            print(f"Error during processing: {e}")
            return False

    def _get_original_data(self) -> Dict[str, Any]:
        return self.minio_connector.fetch_json(BUCKETS["refine"], self.source_file)

    def _transform_data(self, data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        metadata = {
            "source_file": data["source_file"],
            "processing_date": data["processing_date"],
            "processing_version": data["processing_version"],
            "people_count": data["people_count"],
            "processing_timestamp": datetime.utcnow().isoformat() + "Z"
        }

        files = {
            "PersonalInfo": {"metadata": metadata, "people": {}, "statistics": {}},
            "Activity": {"metadata": metadata, "people": {}, "statistics": {}},
            "Security": {"metadata": metadata, "people": {}, "statistics": {}},
            "Confidence": {"metadata": metadata, "people": {}, "statistics": {}},
        }

        gender_dist, age_dist, carrying_dist = {}, {}, {}
        frame_counts, durations, confidences = [], [], []
        restricted_entries = 0

        for person_id, person in data["people"].items():
            # PersonalInfo
            files["PersonalInfo"]["people"][person_id] = {
                "age": person["age"],
                "gender": person["gender"]
            }

            # Activity
            files["Activity"]["people"][person_id] = {
                "first_detection": person["first_detection"],
                "last_detection": person["last_detection"],
                "duration_seconds": person["duration_seconds"],
                "frame_count": person["frame_count"]
            }

            # Security
            files["Security"]["people"][person_id] = {
                "carrying": person["carrying"],
                "entered_restricted_area": person["entered_restricted_area"],
                "restricted_area_entry_time": person["restricted_area_entry_time"]
            }

            # Confidence
            files["Confidence"]["people"][person_id] = {
                "confidence_avg": person["confidence_avg"]
            }

            # Stats aggregation
            gender_dist[person["gender"]] = gender_dist.get(person["gender"], 0) + 1
            age_dist[person["age"]] = age_dist.get(person["age"], 0) + 1
            carrying_dist[person["carrying"]] = carrying_dist.get(person["carrying"], 0) + 1
            frame_counts.append(person["frame_count"])
            durations.append(person["duration_seconds"])
            confidences.append(person["confidence_avg"])
            if person["entered_restricted_area"]:
                restricted_entries += 1

        # Fill statistics
        files["PersonalInfo"]["statistics"] = {
            "gender_distribution": gender_dist,
            "age_distribution": age_dist
        }

        files["Activity"]["statistics"] = {
            "total_frame_count": sum(frame_counts),
            "total_duration_seconds": sum(durations),
            "avg_duration_seconds": mean(durations),
            "avg_frame_count": mean(frame_counts)
        }

        files["Security"]["statistics"] = {
            "carrying_distribution": carrying_dist,
            "restricted_area_entries": restricted_entries
        }

        files["Confidence"]["statistics"] = {
            "avg_confidence": mean(confidences),
            "min_confidence": min(confidences),
            "max_confidence": max(confidences)
        }

        return files

    def _upload_files(self, files: Dict[str, Dict[str, Any]]):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        for feature_name, content in files.items():
            file_path = f"people_detection/{feature_name}/{feature_name}_{timestamp}.json"
            self.minio_connector.write_single_json(content, BUCKETS["refine"], file_path)


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("PeopleDataSplitter").getOrCreate()
    processor = PeopleDataSplitter(spark)
    success = processor.process()
    spark.stop()
    sys.exit(0 if success else 1)
