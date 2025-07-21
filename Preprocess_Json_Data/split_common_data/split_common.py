import json
import os
from datetime import datetime
from collections import defaultdict
from sklearn.cluster import KMeans
import numpy as np

from Preprocess_Json_Data.connectors.split_data_minio_connector import MinIOConnector
from Preprocess_Json_Data.config.minio_config import BUCKETS


class CommonDataSplitter:
    def __init__(self, enable_clustering=False, n_clusters=2):
        self.minio_connector = MinIOConnector(spark=None)
        self.enable_clustering = enable_clustering
        self.n_clusters = n_clusters

    def process(self, filename):
        try:
            self.source_file = f"common_detection/{filename}"
            print(f"Processing {self.source_file} from {BUCKETS['refine']}")

            data = self._get_original_data()
            objects_by_type = self._split_by_class(data)

            result_files = self._process_objects(objects_by_type)
            self._upload_files(result_files)

            print("Processing completed successfully")
            return True

        except Exception as e:
            print(f" Error during processing: {e}")
            return False

    def _get_original_data(self):
        """Fetch original JSON from MinIO"""
        raw = self.minio_connector.fetch_json(BUCKETS["refine"], self.source_file)

        # If it's wrapped like {"data": [...]}, extract it
        if isinstance(raw, dict):
            for value in raw.values():
                if isinstance(value, list):
                    return value
            raise ValueError("Unsupported JSON structure: expected list inside dict")

        if not isinstance(raw, list):
            raise ValueError("Expected JSON to be a list of dicts")

        return raw

    def _split_by_class(self, frames):
        """Extract and group all detection objects by class_name from frames"""
        grouped = defaultdict(list)

        for frame in frames:
            detections = frame.get("detections", [])
            for detection in detections:
                detection["frame_number"] = frame.get("frame_number")
                detection["timestamp"] = frame.get("timestamp")
                grouped[detection["class_name"]].append(detection)

        return grouped

    def _process_objects(self, grouped_data):
        """Apply clustering if needed and prepare files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_files = {}

        for class_name, objects in grouped_data.items():
            if self.enable_clustering and len(objects) >= self.n_clusters:
                # Cluster based on bbox center
                features = [self._bbox_center(obj["bbox"]) for obj in objects]
                kmeans = KMeans(n_clusters=self.n_clusters, random_state=42).fit(features)
                labels = kmeans.labels_

                for cluster_id in range(self.n_clusters):
                    cluster_objs = [obj for i, obj in enumerate(objects) if labels[i] == cluster_id]
                    path = f"common_detection/{class_name}/{class_name}_cluster_{cluster_id}_{timestamp}.json"
                    result_files[path] = cluster_objs
            else:
                # No clustering; single file per class
                path = f"common_detection/{class_name}/{class_name}_{timestamp}.json"
                result_files[path] = objects

        return result_files

    def _upload_files(self, files_dict):
        for path, data in files_dict.items():
            print(f"🟡 Writing file to MinIO → Bucket: {BUCKETS['refine']} | Key: {path}")
            try:
                self.minio_connector.write_single_json(data, BUCKETS["refine"], path)
                print(f"✅ Successfully uploaded: {path}")
            except Exception as e:
                print(f"❌ Failed to upload {path}: {e}")

    def _bbox_center(self, bbox):
        x1, y1, x2, y2 = bbox
        return [(x1 + x2) / 2, (y1 + y2) / 2]
