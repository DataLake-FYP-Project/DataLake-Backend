import json
from datetime import datetime, timezone
import os
from collections import defaultdict
import math
from typing import List, Dict, Any
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from connectors.minio_connector import MinIOConnector
from config.spark_config import create_spark_session
from config.minio_config import BUCKETS


class VehicleDataEnhancer:
    def __init__(self, spark):
        self.minio_connector = MinIOConnector(spark)
        self.warned_tracker_ids = set()
        self.warned_timestamp_format = False
    
    def calculate_distance(self, bbox1: List[float], bbox2: List[float]) -> float:
        """Calculate Euclidean distance between two bounding box centers"""
        x1 = (bbox1[0] + bbox1[2]) / 2
        y1 = (bbox1[1] + bbox1[3]) / 2
        x2 = (bbox2[0] + bbox2[2]) / 2
        y2 = (bbox2[1] + bbox2[3]) / 2
        return math.sqrt((x2 - x1)**2 + (y2 - y1)**2)

    def calculate_trajectory_features(self, track: List[Dict]) -> Dict[str, Any]:
        """Calculate advanced trajectory features for a vehicle track"""
        if len(track) < 2:
            return {}
        
        features = {
            'total_distance': 0,
            'direction_changes': 0,
            'speed_variation': 0,
            'lane_change_frequency': 0,
            'stopped_duration': 0,
            'movement_angles': [],
            'avg_movement_angle': 0
        }
        
        prev_bbox = track[0]['bbox']
        prev_direction = track[0]['direction']
        prev_lane = track[0]['lane']
        prev_speed = track[0]['speed']
        prev_timestamp = track[0]['timestamp']
        
        for i in range(1, len(track)):
            current = track[i]
            try:
                current_timestamp = current['timestamp']
                time_diff = (current_timestamp - prev_timestamp).total_seconds()
                
                distance = self.calculate_distance(prev_bbox, current['bbox'])
                features['total_distance'] += distance
                
                if current['direction'] != prev_direction:
                    features['direction_changes'] += 1
                
                features['speed_variation'] += abs(current['speed'] - prev_speed)
                
                if current['lane'] != prev_lane:
                    features['lane_change_frequency'] += 1
                
                if current.get('stopped', False):
                    features['stopped_duration'] += time_diff
                
                x1 = (prev_bbox[0] + prev_bbox[2]) / 2
                y1 = (prev_bbox[1] + prev_bbox[3]) / 2
                x2 = (current['bbox'][0] + current['bbox'][2]) / 2
                y2 = (current['bbox'][1] + current['bbox'][3]) / 2
                angle = math.degrees(math.atan2(y2 - y1, x2 - x1))
                features['movement_angles'].append(angle)
                
                prev_bbox = current['bbox']
                prev_direction = current['direction']
                prev_lane = current['lane']
                prev_speed = current['speed']
                prev_timestamp = current_timestamp
            except Exception as e:
                print(f"Skipping frame due to error: {e}")
                continue
        
        if len(track) > 1:
            features['speed_variation'] /= (len(track) - 1)
            features['lane_change_frequency'] /= len(track)
            if features['movement_angles']:
                features['avg_movement_angle'] = sum(features['movement_angles']) / len(features['movement_angles'])
        
        return features

    def classify_vehicle_behavior(self, avg_speed: float, speed_variation: float, 
                                lane_change_frequency: float, stopped_duration: float) -> str:
        """Classify vehicle behavior based on movement patterns"""
        if stopped_duration > 10:
            return 'parked'
        elif avg_speed < 5:
            return 'slow_moving'
        elif lane_change_frequency > 0.1:
            return 'aggressive'
        elif speed_variation > 5:
            return 'erratic'
        else:
            return 'normal'

    def parse_timestamp(self, timestamp_str: str, tracker_id: str = "unknown") -> datetime:
        """Flexible timestamp parser with multiple format support"""
        if isinstance(timestamp_str, datetime):
            return timestamp_str
            
        try:
            if 'Z' in timestamp_str or '+' in timestamp_str:
                return datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f%z")
            else:
                if not self.warned_timestamp_format:
                    print("Note: Timestamps missing timezone - assuming UTC")
                    self.warned_timestamp_format = True
                dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")
                return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            try:
                dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                if tracker_id not in self.warned_tracker_ids:
                    print(f"Warning: Invalid timestamp format for tracker_id {tracker_id} - using current time")
                    self.warned_tracker_ids.add(tracker_id)
                return datetime.now(timezone.utc)

    def convert_datetime_to_iso(self, obj):
        """Recursively convert datetime objects to ISO format strings"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: self.convert_datetime_to_iso(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_datetime_to_iso(item) for item in obj]
        return obj

    def extract_vehicle_tracks(self, json_data: List[Dict]) -> List[Dict]:
        """Convert frame-wise detection data into vehicle-wise tracking data"""
        tracks = defaultdict(list)

        for frame in json_data:
            frame_data = frame.get("frame_data", {})
            frame_number = frame_data.get("frame_number", 0)
            frame_time = frame_data.get("frame_time")
            detections = frame_data.get("detections", [])

            for det in detections:
                try:
                    det_copy = det.copy()
                    tracker_id = det_copy.get("tracker_id", "unknown")
                    
                    timestamp_str = (det_copy.get("timestamp") or 
                                   det_copy.get("entry_time") or 
                                   det_copy.get("exit_time") or 
                                   frame_time or 
                                   datetime.utcnow().isoformat())
                    
                    det_copy["timestamp"] = self.parse_timestamp(timestamp_str, tracker_id)
                    det_copy["frame_number"] = frame_number
                    
                    required_fields = ['bbox', 'speed', 'direction', 'lane']
                    if not all(field in det_copy for field in required_fields):
                        if tracker_id not in self.warned_tracker_ids:
                            print(f"Warning: Missing required fields for tracker_id {tracker_id}")
                            self.warned_tracker_ids.add(tracker_id)
                        continue
                        
                    tracks[tracker_id].append(det_copy)
                except Exception as e:
                    print(f"Error processing detection: {e}")
                    continue

        vehicle_tracks = []
        for tracker_id, track_data in tracks.items():
            try:
                track_data = sorted(track_data, key=lambda x: x["frame_number"])
                avg_speed = sum(d['speed'] for d in track_data) / len(track_data) if track_data else 0

                vehicle_tracks.append({
                    "tracker_id": tracker_id,
                    "avg_speed": avg_speed,
                    "track": track_data,
                    "frame_count": len(track_data)
                })
            except Exception as e:
                print(f"Error processing vehicle {tracker_id}: {e}")
                continue

        return vehicle_tracks

    def enhance_vehicle_data(self, vehicle_data: Dict) -> Dict:
        """Enhance vehicle data with advanced features"""
        try:
            enhanced = {
                "metadata": {
                    "tracker_id": vehicle_data["tracker_id"],
                    "processing_version": "2.2"
                },
                "stats": {
                    "avg_speed": vehicle_data["avg_speed"],
                    "frame_count": vehicle_data["frame_count"]
                },
                "track_data": vehicle_data["track"]
            }
            
            trajectory_features = self.calculate_trajectory_features(vehicle_data['track'])
            enhanced["stats"].update(trajectory_features)
            
            enhanced["stats"]["behavior"] = self.classify_vehicle_behavior(
                avg_speed=enhanced["stats"]["avg_speed"],
                speed_variation=enhanced["stats"]["speed_variation"],
                lane_change_frequency=enhanced["stats"]["lane_change_frequency"],
                stopped_duration=enhanced["stats"]["stopped_duration"]
            )
            
            return enhanced
        except Exception as e:
            print(f"Error enhancing vehicle data: {e}")
            raise

    def process_vehicle_data(self, input_data: List[Dict]) -> List[Dict]:
        """Process vehicle data with advanced features"""
        enhanced_vehicles = []
        for vehicle in input_data:
            try:
                enhanced = self.enhance_vehicle_data(vehicle)
                enhanced_vehicles.append(enhanced)
            except Exception as e:
                print(f"Error processing vehicle {vehicle.get('tracker_id')}: {e}")
        
        return enhanced_vehicles

    def process_files(self, input_bucket: str, output_bucket: str):
        """Process all JSON files in the input bucket and save to output bucket"""
        vehicle_files = self.minio_connector.list_json_files(input_bucket, "vehicle_detection/")

        for file_path in vehicle_files:
            print(f"\nProcessing {file_path}...")
            processed_count = 0
            error_count = 0
            
            try:
                # Read and process
                json_data = self.minio_connector.read_json(input_bucket, f"vehicle_detection/{file_path.lstrip('/')}").collect()
                flat_data = [record.asDict(recursive=True) for record in json_data]
                vehicle_tracks = self.extract_vehicle_tracks(flat_data)
                enhanced_data = self.process_vehicle_data(vehicle_tracks)
                processed_count = len(enhanced_data)
                
                # Reformat to {"tracker_id": {...}} format
                vehicles_by_id = {
                    str(v["metadata"]["tracker_id"]): {
                        **v["stats"],
                        "movement_angles": v.get("movement_angles", []),
                        "track_data": v["track_data"]
                    }
                    for v in self.convert_datetime_to_iso(enhanced_data)
                }

                output_data = {
                    "source_file": file_path,
                    "processing_date": datetime.now(timezone.utc).isoformat(),
                    "vehicle_count": processed_count,
                    "vehicles": vehicles_by_id
                }

                # Write to MinIO using existing write_single_json method
                output_path = f"enhanced_vehicles/{os.path.basename(file_path)}"
                self.minio_connector.write_single_json(
                    output_data, 
                    output_bucket, 
                    output_path
                )
                
                print(f"Successfully processed {processed_count} vehicles")
                if error_count > 0:
                    print(f"Skipped {error_count} vehicles due to errors")
                
            except Exception as e:
                print(f"Fatal error processing {file_path}: {e}")

def main():
    spark = create_spark_session()
    try:
        enhancer = VehicleDataEnhancer(spark)
        enhancer.process_files(BUCKETS["processed"], BUCKETS["refine"])
    finally:
        spark.stop()

if __name__ == "__main__":
    main()