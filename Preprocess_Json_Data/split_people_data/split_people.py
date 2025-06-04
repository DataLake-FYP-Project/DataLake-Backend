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
from .minio_connector import MinIOConnector


class PeopleDataSplitter:
    def __init__(self):
        # Initialize MinIO connector without Spark
        self.source_file = None
        self.minio_connector = MinIOConnector(spark=None)  # Pass None since we're not using Spark

    def process(self, filename):
        """Main processing method"""
        try:
            self.source_file = f"people_detection/{filename}"
            print(f"Processing {self.source_file} from {BUCKETS['refine']}")

            # 1. Get original data
            original_data = self._get_original_data()

            # 2. Transform into feature files
            feature_files = self._transform_data(original_data)

            # 3. Upload to MinIO
            self._upload_files(feature_files)

            print("Processing completed successfully")
            return True

        except Exception as e:
            print(f"Error during processing: {e}")
            return False

    def _get_original_data(self) -> Dict[str, Any]:
        """Fetch original JSON from MinIO"""
        return self.minio_connector.fetch_json(BUCKETS["refine"], self.source_file)

    def _transform_data(self, data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Transform the data into feature-based files"""
        try:
            # Prepare metadata
            metadata = {
                "source_file": data.get("source_file", "unknown"),
                "processing_date": data.get("processing_date", "unknown"),
                "processing_version": data.get("processing_version", "unknown"),
                "people_count": data.get("people_count", 0),
            }

            # Initialize files structure
            files = {
                "PersonalInfo": {"metadata": metadata, "people": {}, "statistics": {}},
                "Activity": {"metadata": metadata, "people": {}, "statistics": {}},
                "Security": {"metadata": metadata, "people": {}, "statistics": {}},
                "Confidence": {"metadata": metadata, "people": {}, "statistics": {}},
            }

            # Initialize statistics
            gender_dist = {}
            age_dist = {}
            carrying_dist = {}
            frame_counts = []
            durations = []
            confidences = []
            restricted_entries = 0
            valid_person_found = False

            # Process each person
            for person_id, person in data.get("people", {}).items():
                # Initialize all person data containers
                personal_info, activity, security, confidence = {}, {}, {}, {}

                # Personal Info
                age = person.get("age")
                gender = person.get("gender")
                if age is not None and gender is not None:
                    personal_info = {"age": age, "gender": gender}
                    gender_dist[gender] = gender_dist.get(gender, 0) + 1
                    age_dist[age] = age_dist.get(age, 0) + 1
                    valid_person_found = True

                # Activity
                if all(k in person for k in ["first_detection", "last_detection", "duration_seconds", "frame_count"]):
                    activity = {
                        "first_detection": person["first_detection"],
                        "last_detection": person["last_detection"],
                        "duration_seconds": person["duration_seconds"],
                        "frame_count": person["frame_count"]
                    }
                    durations.append(person["duration_seconds"])
                    frame_counts.append(person["frame_count"])
                    valid_person_found = True

                # Security
                if all(k in person for k in ["carrying", "entered_restricted_area", "restricted_area_entry_time"]):
                    security = {
                        "carrying": person["carrying"],
                        "entered_restricted_area": person["entered_restricted_area"],
                        "restricted_area_entry_time": person["restricted_area_entry_time"]
                    }
                    carrying_dist[person["carrying"]] = carrying_dist.get(person["carrying"], 0) + 1
                    if person["entered_restricted_area"]:
                        restricted_entries += 1
                    valid_person_found = True

                # Confidence
                confidence_avg = person.get("confidence_avg")
                if confidence_avg is not None:
                    confidence = {"confidence_avg": confidence_avg}
                    confidences.append(confidence_avg)
                    valid_person_found = True

                if personal_info:
                    files["PersonalInfo"]["people"][person_id] = personal_info
                if activity:
                    files["Activity"]["people"][person_id] = activity
                if security:
                    files["Security"]["people"][person_id] = security
                if confidence:
                    files["Confidence"]["people"][person_id] = confidence

            if not valid_person_found:
                print("No valid person data found. Skipping processing.")
                return {}

            # Calculate and add statistics
            files["PersonalInfo"]["statistics"] = {
                "gender_distribution": gender_dist,
                "age_distribution": age_dist
            }

            files["Activity"]["statistics"] = {
                "total_frame_count": sum(frame_counts),
                "total_duration_seconds": sum(durations),
                "avg_duration_seconds": mean(durations) if durations else 0,
                "avg_frame_count": mean(frame_counts) if frame_counts else 0
            }

            files["Security"]["statistics"] = {
                "carrying_distribution": carrying_dist,
                "restricted_area_entries": restricted_entries
            }

            files["Confidence"]["statistics"] = {
                "avg_confidence": mean(confidences) if confidences else 0,
                "min_confidence": min(confidences) if confidences else 0,
                "max_confidence": max(confidences) if confidences else 0
            }

            return files
        except Exception as e:
            print(f"Error during data transformation: {e}")
            return {}

    def _upload_files(self, files: Dict[str, Dict[str, Any]]):
        """Upload the generated files to MinIO"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        for feature_name, content in files.items():
            file_path = f"people_detection/{feature_name}/{feature_name}_{timestamp}.json"
            self.minio_connector.write_single_json(content, BUCKETS["refine"], file_path)

