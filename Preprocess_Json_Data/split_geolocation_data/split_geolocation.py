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
from Preprocess_Json_Data.connectors.split_data_minio_connector import MinIOConnector


class GeolocationDataSplitter:
    def __init__(self):
        self.minio_connector = MinIOConnector(spark=None)
        self.source_file = None

    def process(self, filename):
        try:
            self.source_file = f"geolocation_detection/{filename}"
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
                "source_file": self.source_file,
                "processing_date": datetime.now().strftime("%Y-%m-%d"),
                "processing_version": "1.0",
                "processing_timestamp": datetime.utcnow().isoformat() + "Z"
            }

            # Initialize files structure
            files = {
                "Geolocation": {"metadata": metadata, "frames": {}, "statistics": {}},
                "BoundingBox": {"metadata": metadata, "frames": {}, "statistics": {}},
                "Confidence": {"metadata": metadata, "frames": {}, "statistics": {}},
            }

            # Initialize statistics
            class_counts = {}
            confidences = []

            # Process each frame detection
            frames = data["frame_detections"]
            for frame in frames:
                frame_num = frame["frame_number"]
                geo = frame["geolocation"]
                bbox = frame["bbox"]
                confidence = frame["confidence"]
                class_name = frame["class"]

                # Geolocation
                files["Geolocation"]["frames"][str(frame_num)] = {
                    "latitude": geo["latitude"],
                    "longitude": geo["longitude"]
                }

                # BoundingBox
                files["BoundingBox"]["frames"][str(frame_num)] = {
                    "x1": bbox[0],
                    "y1": bbox[1],
                    "x2": bbox[2],
                    "y2": bbox[3]
                }

                # Confidence
                files["Confidence"]["frames"][str(frame_num)] = {
                    "confidence": confidence
                }

                # Update statistics
                class_counts[class_name] = class_counts.get(class_name, 0) + 1
                confidences.append(confidence)

            # Calculate and add statistics
            files["Geolocation"]["statistics"] = {
                "total_frames": len(frames)
            }

            files["BoundingBox"]["statistics"] = {
                "class_distribution": class_counts
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
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        for feature_name, content in files.items():
            file_path = f"geolocation_detection/{feature_name}/{feature_name}_{timestamp}.json"
            self.minio_connector.write_single_json(content, BUCKETS["refine"], file_path)