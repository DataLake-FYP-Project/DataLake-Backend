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


class PoseDataSplitter:
    def __init__(self):
        self.minio_connector = MinIOConnector(spark=None)
        self.source_file = None

    def process(self, filename):
        try:
            self.source_file = f"pose_detection/{filename}"
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
                "PoseInfo": {"metadata": metadata, "frames": {}, "statistics": {}},
                "Movement": {"metadata": metadata, "frames": {}, "statistics": {}},
                "Confidence": {"metadata": metadata, "frames": {}, "statistics": {}},
            }

            # Initialize statistics
            actions_count = {}
            confidences = []

            # Process each frame
            frames = data["frame_detections"]
            for frame in frames:
                frame_num = frame["frame_number"]
                action = frame.get("action", "Unknown")
                confidence = frame.get("confidence", 0.0)

                # PoseInfo
                files["PoseInfo"]["frames"][str(frame_num)] = {
                    "frame_number": frame_num,
                    "duration_seconds": 0.033 if frame_num > 0 and frame_num - 1 not in [f["frame_number"] for f in
                                                                                         frames[
                                                                                         :frames.index(frame)]] else 0
                    # Approx 30 FPS
                }

                # Movement
                files["Movement"]["frames"][str(frame_num)] = {
                    "action": action
                }

                # Confidence
                files["Confidence"]["frames"][str(frame_num)] = {
                    "confidence": confidence
                }

                # Update statistics
                actions_count[action] = actions_count.get(action, 0) + 1
                confidences.append(confidence)

            # Calculate and add statistics
            files["PoseInfo"]["statistics"] = {
                "total_frames": len(frames),
                "avg_duration_seconds": mean(
                    [f["duration_seconds"] for f in files["PoseInfo"]["frames"].values()]) if frames else 0
            }

            files["Movement"]["statistics"] = {
                "action_distribution": actions_count
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
            file_path = f"pose_detection/{feature_name}/{feature_name}_{timestamp}.json"
            self.minio_connector.write_single_json(content, BUCKETS["refine"], file_path)

