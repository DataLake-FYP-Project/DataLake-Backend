import json
from datetime import datetime
from statistics import mean
import sys
from typing import Dict, Any
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Add project root to Python path
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

from Preprocess_Json_Data.config.minio_config import BUCKETS
from minio_connector import MinIOConnector


class SafetyDataSplitter:
    def __init__(self):
        self.minio_connector = MinIOConnector(spark=None)
        self.source_file = "safety_detection/refine_safety_video_2025-06-01_22-30-12.json"

    def process(self):
        try:
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
        data = self.minio_connector.fetch_json(BUCKETS["refine"], self.source_file)
        return data

    def _transform_data(self, data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        try:
            # Prepare metadata
            metadata = {
                "source_file": data["source_file"],
                "processing_date": datetime.now().strftime("%Y-%m-%d"),
                "processing_version": "1.0",
                "processing_timestamp": datetime.utcnow().isoformat() + "Z"
            }

            # Initialize files structure
            files = {
                "SafetyInfo": {"metadata": metadata, "objects": {}, "statistics": {}},
                "Violations": {"metadata": metadata, "objects": {}, "statistics": {}},
                "GearStatus": {"metadata": metadata, "objects": {}, "statistics": {}},
                "Summary": {"metadata": metadata, "objects": {}, "statistics": {}},
            }

            # Initialize statistics
            total_frames_list = []
            bbox_movement_list = []
            violation_counts = {"hardhat": [], "mask": [], "safety_vest": []}
            gear_presence = {"hardhat": 0, "mask": 0, "safety_vest": 0}
            missing_items_count = {}
            safety_status_count = {"Safe": 0, "Unsafe": 0}

            # Process each object
            safety_objects = data["safety_objects"]
            for obj_id, obj_data in safety_objects.items():
                # SafetyInfo
                files["SafetyInfo"]["objects"][obj_id] = {
                    "total_frames": obj_data["total_frames"],
                    "duration_frames": obj_data["duration_frames"],
                    "bbox_count": obj_data["bbox_count"],
                    "bbox_movement_estimate": obj_data["bbox_movement_estimate"]
                }

                # Violations
                files["Violations"]["objects"][obj_id] = {
                    "hardhat_violations": obj_data["hardhat_violations"],
                    "mask_violations": obj_data["mask_violations"],
                    "safety_vest_violations": obj_data["safety_vest_violations"],
                    "total_unsafe_frames": obj_data["total_unsafe_frames"]
                }

                # GearStatus
                files["GearStatus"]["objects"][obj_id] = {
                    "hardhat": obj_data["hardhat"],
                    "mask": obj_data["mask"],
                    "safety_vest": obj_data["safety_vest"]
                }

                # Summary
                files["Summary"]["objects"][obj_id] = {
                    "initial_safety_status": obj_data["initial_safety_status"],
                    "safety_status": obj_data["safety_status"],
                    "most_common_missing_item": obj_data["most_common_missing_item"]
                }

                # Update statistics
                total_frames_list.append(obj_data["total_frames"])
                bbox_movement_list.append(obj_data["bbox_movement_estimate"])
                violation_counts["hardhat"].append(obj_data["hardhat_violations"])
                violation_counts["mask"].append(obj_data["mask_violations"])
                violation_counts["safety_vest"].append(obj_data["safety_vest_violations"])

                if obj_data["hardhat"]:
                    gear_presence["hardhat"] += 1
                if obj_data["mask"]:
                    gear_presence["mask"] += 1
                if obj_data["safety_vest"]:
                    gear_presence["safety_vest"] += 1

                missing_items_count[obj_data["most_common_missing_item"]] = missing_items_count.get(obj_data["most_common_missing_item"], 0) + 1
                safety_status_count[obj_data["safety_status"]] = safety_status_count.get(obj_data["safety_status"], 0) + 1

            # Calculate and add statistics
            files["SafetyInfo"]["statistics"] = {
                "total_objects": len(safety_objects),
                "avg_total_frames": mean(total_frames_list) if total_frames_list else 0,
                "avg_bbox_movement": mean(bbox_movement_list) if bbox_movement_list else 0
            }

            files["Violations"]["statistics"] = {
                "avg_hardhat_violations": mean(violation_counts["hardhat"]) if violation_counts["hardhat"] else 0,
                "avg_mask_violations": mean(violation_counts["mask"]) if violation_counts["mask"] else 0,
                "avg_safety_vest_violations": mean(violation_counts["safety_vest"]) if violation_counts["safety_vest"] else 0
            }

            files["GearStatus"]["statistics"] = {
                "hardhat_presence_count": gear_presence["hardhat"],
                "mask_presence_count": gear_presence["mask"],
                "safety_vest_presence_count": gear_presence["safety_vest"]
            }

            files["Summary"]["statistics"] = {
                "safety_status_distribution": safety_status_count,
                "most_common_missing_item_distribution": missing_items_count
            }

            return files
        except Exception as e:
            print(f"Error during data transformation: {e}")
            return {}

    def _upload_files(self, files: Dict[str, Dict[str, Any]]):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        for feature_name, content in files.items():
            file_path = f"safety_detection/{feature_name}/{feature_name}_{timestamp}.json"
            self.minio_connector.write_single_json(content, BUCKETS["refine"], file_path)


if __name__ == "__main__":
    processor = SafetyDataSplitter()
    success = processor.process()
    sys.exit(0 if success else 1)