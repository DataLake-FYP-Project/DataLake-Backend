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


class VehicleDataSplitter:
    def __init__(self):
        # Initialize MinIO connector without Spark
        self.minio_connector = MinIOConnector(spark=None)  # Pass None since we're not using Spark
        self.source_file = "vehicle_detection/refine_vehicle-counting1_2025-05-29_14-45-44.json"

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
        return self.minio_connector.fetch_json(BUCKETS["refine"], self.source_file)

    def _transform_data(self, data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Transform the data into feature-based files"""
        # Prepare metadata
        metadata = {
            "source_file": data["source_file"],
            "processing_date": data["processing_date"],
            "processing_version": data["processing_version"],
            "vehicle_count": data["vehicle_count"],
            "processing_timestamp": datetime.utcnow().isoformat() + "Z"
        }

        # Initialize files structure
        files = {
            "VehicleInfo": {"metadata": metadata, "vehicles": {}, "statistics": {}},
            "Movement": {"metadata": metadata, "vehicles": {}, "statistics": {}},
            "Violations": {"metadata": metadata, "vehicles": {}, "statistics": {}},
            "Confidence": {"metadata": metadata, "vehicles": {}, "statistics": {}},
        }

        # Initialize statistics
        lane_change_dist, direction_change_dist = {}, {}
        stopped_durations, red_light_violations = [], []
        frame_counts, durations, confidences = [], [], []

        # Process each vehicle
        for vehicle_id, vehicle in data["vehicles"].items():
            # VehicleInfo
            files["VehicleInfo"]["vehicles"][vehicle_id] = {
                "first_detection": vehicle["first_detection"],
                "last_detection": vehicle["last_detection"],
                "duration_seconds": vehicle["duration_seconds"],
                "frame_count": vehicle["frame_count"]
            }

            # Movement
            files["Movement"]["vehicles"][vehicle_id] = {
                "lane_changes": vehicle["lane_changes"],
                "direction_changes": vehicle["direction_changes"]
            }

            # Violations
            files["Violations"]["vehicles"][vehicle_id] = {
                "stopped_duration": vehicle["stopped_duration"],
                "red_light_violation_count": vehicle["red_light_violation_count"]
            }

            # Confidence
            files["Confidence"]["vehicles"][vehicle_id] = {
                "confidence_avg": vehicle["confidence_avg"]
            }

            # Update statistics
            lane_change_dist[vehicle["lane_changes"]] = lane_change_dist.get(vehicle["lane_changes"], 0) + 1
            direction_change_dist[vehicle["direction_changes"]] = direction_change_dist.get(
                vehicle["direction_changes"], 0) + 1
            stopped_durations.append(vehicle["stopped_duration"])
            red_light_violations.append(vehicle["red_light_violation_count"])
            frame_counts.append(vehicle["frame_count"])
            durations.append(vehicle["duration_seconds"])
            confidences.append(vehicle["confidence_avg"])

        # Calculate and add statistics
        files["VehicleInfo"]["statistics"] = {
            "total_frame_count": sum(frame_counts),
            "total_duration_seconds": sum(durations),
            "avg_duration_seconds": mean(durations) if durations else 0,
            "avg_frame_count": mean(frame_counts) if frame_counts else 0
        }

        files["Movement"]["statistics"] = {
            "lane_change_distribution": lane_change_dist,
            "direction_change_distribution": direction_change_dist
        }

        files["Violations"]["statistics"] = {
            "total_stopped_duration": sum(stopped_durations),
            "avg_stopped_duration": mean(stopped_durations) if stopped_durations else 0,
            "total_red_light_violations": sum(red_light_violations),
            "vehicles_with_red_light_violations": sum(1 for v in red_light_violations if v > 0)
        }

        files["Confidence"]["statistics"] = {
            "avg_confidence": mean(confidences) if confidences else 0,
            "min_confidence": min(confidences) if confidences else 0,
            "max_confidence": max(confidences) if confidences else 0
        }

        return files

    def _upload_files(self, files: Dict[str, Dict[str, Any]]):
        """Upload the generated files to MinIO"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        for feature_name, content in files.items():
            file_path = f"vehicle_detection/{feature_name}/{feature_name}_{timestamp}.json"
            self.minio_connector.write_single_json(content, BUCKETS["refine"], file_path)


if __name__ == "__main__":
    processor = VehicleDataSplitter()
    success = processor.process()
    sys.exit(0 if success else 1)