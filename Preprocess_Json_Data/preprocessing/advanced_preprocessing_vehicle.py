import json
from datetime import datetime, timezone
import os
from pathlib import Path
import sys
from typing import List, Dict, Any
sys.path.append(str(Path(__file__).parent.parent))

from connectors.minio_connector import MinIOConnector
from config.spark_config import create_spark_session
from config.minio_config import BUCKETS
from pyspark.sql.functions import col, min as spark_min, max as spark_max, avg, stddev, count, collect_list, expr, udf, explode
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
import math

class VehicleProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.minio = MinIOConnector(spark)

    def process_files(self, input_bucket: str, output_bucket: str):
        files = self.minio.list_json_files(input_bucket, "vehicle_detection/")
        for file in files:
            print(f"Processing {file}...")
            df = self.minio.read_json(input_bucket, f"vehicle_detection/{file}")

            # Handle nested format with frame_data.detections
            if "frame_data" in df.columns:
                df = df.selectExpr("frame_data.frame_number as frame_number", "explode(frame_data.detections) as detection")
                df = df.select(
                    "frame_number",
                    "detection.tracker_id",
                    "detection.bbox",
                    "detection.speed",
                    "detection.direction",
                    "detection.lane",
                    "detection.stopped",
                    "detection.confidence",
                    "detection.class_name",
                    "detection.vehicle_color",
                    F.current_timestamp().alias("timestamp")
                )

            # Validate required fields
            if 'tracker_id' not in df.columns or 'timestamp' not in df.columns:
                print(f"Skipping {file}: Missing essential fields.")
                continue

            df = df.withColumn("timestamp", F.to_timestamp("timestamp"))

            grouped = df.groupBy("tracker_id").agg(
                spark_min("timestamp").alias("first_detection"),
                spark_max("timestamp").alias("last_detection"),
                count("timestamp").alias("frame_count"),
                avg("speed").alias("avg_speed"),
                spark_max("speed").alias("max_speed"),
                spark_min("speed").alias("min_speed"),
                avg("confidence").alias("confidence_avg"),
                F.first("class_name", ignorenulls=True).alias("vehicle_type"),
                F.first("vehicle_color", ignorenulls=True).alias("vehicle_color"),
                F.first("lane", ignorenulls=True).alias("initial_lane"),
                F.last("lane", ignorenulls=True).alias("final_lane"),
                F.last("direction", ignorenulls=True).alias("direction"),
                collect_list("lane").alias("lanes"),
                collect_list("direction").alias("directions"),
                collect_list("speed").alias("speed_list"),
                collect_list("stopped").alias("stopped_list"),
                collect_list("bbox").alias("bbox_list"),
                F.collect_list("timestamp").alias("timestamps"),
                F.collect_list(F.struct(df.columns)).alias("track_data")
            )

            def enrich(row):
                import statistics

                tid = str(row["tracker_id"])
                lanes = row["lanes"] or []
                directions = row["directions"] or []
                speeds = row["speed_list"] or []
                bbox_list = row["bbox_list"] or []
                timestamps = row["timestamps"] or []
                stopped_list = row["stopped_list"] or []

                lane_changes = sum(1 for i in range(1, len(lanes)) if lanes[i] != lanes[i - 1])
                lane_change_frequency = lane_changes / len(lanes) if lanes else 0
                direction_changes = sum(1 for i in range(1, len(directions)) if directions[i] != directions[i - 1])
                stopped_duration = 0.0

                for s, t1, t2 in zip(stopped_list, timestamps[:-1], timestamps[1:]):
                    if s and t1 and t2:
                        td = (t2.timestamp() - t1.timestamp())
                        stopped_duration += td

                movement_angles = []
                total_distance = 0.0
                for i in range(1, len(bbox_list)):
                    try:
                        b1 = bbox_list[i - 1]
                        b2 = bbox_list[i]
                        x1 = (b1[0] + b1[2]) / 2
                        y1 = (b1[1] + b1[3]) / 2
                        x2 = (b2[0] + b2[2]) / 2
                        y2 = (b2[1] + b2[3]) / 2
                        dist = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)
                        total_distance += dist
                        angle = math.degrees(math.atan2(y2 - y1, x2 - x1))
                        movement_angles.append(angle)
                    except:
                        continue

                avg_movement_angle = sum(movement_angles) / len(movement_angles) if movement_angles else 0.0
                speed_variation = statistics.stdev(speeds) if len(speeds) >= 2 else 0.0

                return tid, {
                    "vehicle_type": row["vehicle_type"] or "Unknown",
                    "vehicle_color": row["vehicle_color"] or "Unknown",
                    "first_detection": row["first_detection"].isoformat(),
                    "last_detection": row["last_detection"].isoformat(),
                    "duration_seconds": (row["last_detection"] - row["first_detection"]).total_seconds(),
                    "frame_count": row["frame_count"],
                    "avg_speed": row["avg_speed"] or 0.0,
                    "max_speed": row["max_speed"] or 0.0,
                    "min_speed": row["min_speed"] or 0.0,
                    "lane_changes": lane_changes,
                    "initial_lane": row["initial_lane"] or "Unknown",
                    "final_lane": row["final_lane"] or "Unknown",
                    "direction": row["direction"] or "Unknown",
                    "confidence_avg": row["confidence_avg"] or 0.0,
                    "total_distance": total_distance,
                    "direction_changes": direction_changes,
                    "speed_variation": speed_variation,
                    "lane_change_frequency": lane_change_frequency,
                    "stopped_duration": stopped_duration,
                    "movement_angles": movement_angles,
                    "avg_movement_angle": avg_movement_angle,
                    "track_data": [{**r.asDict(), "timestamp": r["timestamp"].isoformat()} for r in row["track_data"]]
                }

            enriched_data = dict(sorted([enrich(row) for row in grouped.collect()], key=lambda x: int(x[0])))

            output = {
                "source_file": file,
                "processing_date": datetime.now(timezone.utc).isoformat(),
                "vehicle_count": len(enriched_data),
                "vehicles": enriched_data
            }

            out_path = f"enhanced_vehicles/{file}"
            self.minio.write_single_json(output, output_bucket, out_path)

if __name__ == '__main__':
    spark = create_spark_session()
    processor = VehicleProcessor(spark)
    processor.process_files(BUCKETS["processed"], BUCKETS["refine"])
    spark.stop()
