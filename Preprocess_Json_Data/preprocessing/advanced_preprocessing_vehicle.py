import json
import sys
import math
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from connectors.minio_connector import MinIOConnector
from config.spark_config import create_spark_session
from config.minio_config import BUCKETS
from pyspark.sql.functions import col, min as spark_min, max as spark_max, avg, stddev, count, collect_list, expr, udf, explode
from pyspark.sql import functions as F


class VehicleProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.minio = MinIOConnector(spark)

    def process_files(self, input_bucket: str, output_bucket: str):
        files = self.minio.list_json_files(input_bucket, "vehicle_detection/")
        for file in files:
            print(f"\nProcessing {file}...")
            try:
                # Read the JSON file
                df = self.minio.read_json(input_bucket, f"vehicle_detection/{file}")
                
                # Explode the detections array
                df = df.select(
                    F.col("frame_data.frame_number").alias("frame_number"),
                    F.explode("frame_data.detections").alias("detection")
                )
                
                # Extract all fields from the detection struct
                df = df.select(
                    "frame_number",
                    F.col("detection.tracker_id").alias("tracker_id"),
                    F.coalesce(
                        F.col("detection.entry_time"),
                        F.col("detection.exit_time"),
                        F.lit(datetime.now(timezone.utc).isoformat())
                    ).alias("timestamp"),
                    F.col("detection.bbox").alias("bbox"),
                    F.col("detection.speed").alias("speed"),
                    F.col("detection.direction").alias("direction"),
                    F.col("detection.lane").alias("lane"),
                    F.col("detection.stopped").alias("stopped"),
                    F.col("detection.confidence").alias("confidence"),
                    F.col("detection.class_name").alias("class_name"),
                    F.col("detection.vehicle_color").alias("vehicle_color")
                )
                
                # Convert timestamp to proper type
                df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
                
                # Group by tracker_id and aggregate
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
                    collect_list("vehicle_color").alias("color_list"),
                    collect_list("speed").alias("speed_list"),
                    collect_list("stopped").alias("stopped_list"),
                    collect_list("bbox").alias("bbox_list"),
                    F.collect_list("timestamp").alias("timestamps")
                )

                def enrich(row):
                    import statistics
                    from collections import Counter, defaultdict

                    tid = str(row["tracker_id"])
                    lanes = row["lanes"] or []
                    directions = row["directions"] or []
                    speeds = row["speed_list"] or []
                    bbox_list = row["bbox_list"] or []
                    timestamps = row["timestamps"] or []
                    stopped_list = row["stopped_list"] or []
                    colors = row["color_list"] or []

                    # Calculate most common values
                    lane_counts = Counter(lanes)
                    most_common_lane = lane_counts.most_common(1)[0][0] if lane_counts else "Unknown"
                    direction_counts = Counter(directions)
                    most_common_direction = direction_counts.most_common(1)[0][0] if direction_counts else "Unknown"
                    color_counts = Counter(colors)
                    most_common_color = color_counts.most_common(1)[0][0] if color_counts else row["vehicle_color"] or "Unknown"

                    # Calculate movement metrics
                    lane_changes = sum(1 for i in range(1, len(lanes)) if lanes[i] != lanes[i-1])
                    lane_change_frequency = lane_changes / len(lanes) if lanes else 0
                    direction_changes = sum(1 for i in range(1, len(directions)) if directions[i] != directions[i-1])
                    stopped_duration = 0.0

                    # Calculate time spent per direction
                    direction_time = defaultdict(float)
                    for i in range(1, len(directions)):
                        d1 = directions[i-1]
                        t1, t2 = timestamps[i-1], timestamps[i]
                        if d1 and t1 and t2:
                            delta = (t2.timestamp() - t1.timestamp())
                            direction_time[d1] += delta

                    # Calculate stopped duration
                    for s, t1, t2 in zip(stopped_list, timestamps[:-1], timestamps[1:]):
                        if s and t1 and t2:
                            td = (t2.timestamp() - t1.timestamp())
                            stopped_duration += td

                    # Calculate movement angles and distance
                    movement_angles = []
                    total_distance = 0.0
                    for i in range(1, len(bbox_list)):
                        try:
                            b1 = bbox_list[i-1]
                            b2 = bbox_list[i]
                            x1 = (b1[0] + b1[2]) / 2
                            y1 = (b1[1] + b1[3]) / 2
                            x2 = (b2[0] + b2[2]) / 2
                            y2 = (b2[1] + b2[3]) / 2
                            dist = math.sqrt((x2-x1)**2 + (y2-y1)**2)
                            total_distance += dist
                            angle = math.degrees(math.atan2(y2-y1, x2-x1))
                            movement_angles.append(angle)
                        except:
                            continue

                    avg_movement_angle = sum(movement_angles)/len(movement_angles) if movement_angles else 0.0
                    speed_variation = statistics.stdev(speeds) if len(speeds) >= 2 else 0.0

                    return tid, {
                        "vehicle_type": row["vehicle_type"] or "Unknown",
                        "vehicle_color": most_common_color,
                        "confidence_avg": row["confidence_avg"] or 0.0,
                        "first_detection": row["first_detection"].isoformat(),
                        "last_detection": row["last_detection"].isoformat(),
                        "duration_seconds": (row["last_detection"] - row["first_detection"]).total_seconds(),
                        "stopped_duration": stopped_duration,
                        "frame_count": row["frame_count"],
                        "avg_speed": row["avg_speed"] or 0.0,
                        "max_speed": row["max_speed"] or 0.0,
                        "min_speed": row["min_speed"] or 0.0,
                        "speed_variation": speed_variation,
                        "lane_changes": lane_changes,
                        "initial_lane": row["initial_lane"] or "Unknown",
                        "final_lane": row["final_lane"] or "Unknown",
                        "most_common_lane": most_common_lane,
                        "lane_change_frequency": lane_change_frequency,
                        "direction": most_common_direction,
                        "direction_changes": direction_changes,
                        "time_spent_per_direction": dict(direction_time),
                        "total_distance": total_distance,
                        "movement_angles": movement_angles,
                        "avg_movement_angle": avg_movement_angle
                    }

                # Process and save the data
                enriched_data = dict(sorted([enrich(row) for row in grouped.collect()], key=lambda x: int(x[0])))
                
                output = {
                    "source_file": file,
                    "processing_date": datetime.now(timezone.utc).isoformat(),
                    "vehicle_count": len(enriched_data),
                    "vehicles": enriched_data
                }

                out_path = f"refine_{file}"
                self.minio.write_single_json(output, output_bucket, out_path)
                print(f"Successfully processed {len(enriched_data)} vehicles in {file}")

            except Exception as e:
                print(f"Error processing file {file}: {str(e)}")
                continue

if __name__ == '__main__':
    spark = create_spark_session()
    processor = VehicleProcessor(spark)
    processor.process_files(BUCKETS["processed"], BUCKETS["refine"])
    spark.stop()