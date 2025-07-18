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
from Preprocess_Json_Data.connectors.split_data_minio_connector import MinIOConnector


class AnimalDataSplitter:
    def __init__(self):
        self.source_file = None
        self.minio_connector = MinIOConnector(spark=None)

    def process(self, filename):
        try:
            self.source_file = f"animal_detection/{filename}"
            print(f"[INFO] Processing {self.source_file} from bucket: {BUCKETS['refine']}")

            original_data = self._get_original_data()
            if not original_data:
                print("[ERROR] No data fetched from MinIO.")
                return False

            feature_files = self._transform_data(original_data)
            if not feature_files:
                print("[WARN] No valid animal data found. Nothing to upload.")
                return False

            self._upload_files(feature_files)
            print("[SUCCESS] Processing completed successfully.")
            return True

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"[ERROR] Exception during processing: {e}")
            return False

    def _get_original_data(self) -> Dict[str, Any]:
        try:
            print(f"[DEBUG] Fetching file: {self.source_file}")
            data = self.minio_connector.fetch_json(BUCKETS["refine"], self.source_file)
            print(f"[DEBUG] Fetched data keys: {list(data.keys())}")
            return data
        except Exception as e:
            print(f"[ERROR] Failed to fetch original data: {e}")
            raise

    def _transform_data(self, data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        try:
            print("[INFO] Transforming frame_detections into animal features...")
            frame_detections = data.get("frame_detections", [])
            if not frame_detections:
                print("[WARN] No frame_detections found.")
                return {}

            metadata = {
                "source_file": self.source_file,
                "processing_date": datetime.now().strftime("%Y-%m-%d"),
                "processing_version": "v1.0",
                "animal_count": 0
            }

            files = {
                "AnimalInfo": {"metadata": metadata, "animals": {}, "statistics": {}},
                "DetectionActivity": {"metadata": metadata, "animals": {}, "statistics": {}},
                "Confidence": {"metadata": metadata, "animals": {}, "statistics": {}},
            }

            # Group detections by class_name
            animal_stats = {}
            for frame in frame_detections:
                frame_data = frame.get("frame_data", {})
                frame_num = frame_data.get("frame_number")
                timestamp = frame_data.get("timestamp")
                detections = frame_data.get("detections", [])

                for det in detections:
                    class_name = det.get("class_name")
                    confidence = det.get("confidence")

                    if not class_name or confidence is None:
                        continue

                    if class_name not in animal_stats:
                        animal_stats[class_name] = {
                            "first_detection": timestamp,
                            "last_detection": timestamp,
                            "frame_count": 1,
                            "confidences": [confidence]
                        }
                    else:
                        stats = animal_stats[class_name]
                        stats["last_detection"] = timestamp
                        stats["frame_count"] += 1
                        stats["confidences"].append(confidence)

            if not animal_stats:
                print("[WARN] No valid animal detections found.")
                return {}

            durations = []
            frame_counts = []
            confidences_all = []

            for i, (class_name, stats) in enumerate(animal_stats.items(), start=1):
                animal_id = f"animal_{i}"
                duration = stats["last_detection"] - stats["first_detection"]
                avg_conf = mean(stats["confidences"])
                confidences_all.extend(stats["confidences"])

                # AnimalInfo
                files["AnimalInfo"]["animals"][animal_id] = {
                    "class_name": class_name
                }

                # DetectionActivity
                files["DetectionActivity"]["animals"][animal_id] = {
                    "first_detection": stats["first_detection"],
                    "last_detection": stats["last_detection"],
                    "duration_seconds": duration,
                    "frame_count": stats["frame_count"]
                }

                # Confidence
                files["Confidence"]["animals"][animal_id] = {
                    "confidence_avg": avg_conf
                }

                durations.append(duration)
                frame_counts.append(stats["frame_count"])

            metadata["animal_count"] = len(animal_stats)

            files["AnimalInfo"]["statistics"] = {
                "class_distribution": {k: 1 for k in animal_stats}
            }
            files["DetectionActivity"]["statistics"] = {
                "total_frame_count": sum(frame_counts),
                "total_duration_seconds": sum(durations),
                "avg_duration_seconds": mean(durations) if durations else 0,
                "avg_frame_count": mean(frame_counts) if frame_counts else 0
            }
            files["Confidence"]["statistics"] = {
                "avg_confidence": mean(confidences_all) if confidences_all else 0,
                "min_confidence": min(confidences_all) if confidences_all else 0,
                "max_confidence": max(confidences_all) if confidences_all else 0
            }

            return files

        except Exception as e:
            print(f"[ERROR] Error during data transformation: {e}")
            raise

    def _upload_files(self, files: Dict[str, Dict[str, Any]]):
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            print(f"[INFO] Uploading {len(files)} files to MinIO...")

            for feature_name, content in files.items():
                file_path = f"animal_detection/{feature_name}/{feature_name}_{timestamp}.json"
                print(f"[DEBUG] Uploading: {file_path}")
                self.minio_connector.write_single_json(content, BUCKETS["refine"], file_path)

        except Exception as e:
            print(f"[ERROR] Upload failed: {e}")
            raise
