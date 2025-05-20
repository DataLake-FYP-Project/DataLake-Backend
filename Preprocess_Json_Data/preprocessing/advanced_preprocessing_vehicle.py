# import json
# import sys
# import math
# from datetime import datetime, timezone
# from pathlib import Path
# import statistics
# import logging
# from collections import Counter, defaultdict

# sys.path.append(str(Path(__file__).parent.parent))
# from ..connectors.minio_connector import MinIOConnector
# from pyspark.sql.functions import col, min as spark_min, max as spark_max, avg, stddev, count, collect_list, expr, udf, \
#     explode, lit, array, coalesce
# from pyspark.sql import functions as F


# class VehicleProcessor:
#     def __init__(self, spark):
#         self.spark = spark
#         self.minio = MinIOConnector(spark)

#     def _process_vehicle_format(self, df):
#         """Process vehicle detection format with robust error handling"""
#         try:
#             # Validate required columns
#             if "frame_data" not in df.columns:
#                 logging.info("Missing required column: frame_data")
#                 return None

#             # Initial processing with required columns
#             processed = df.select(
#                 F.col("frame_data.frame_number").alias("frame_number"),
#                 F.explode("frame_data.detections").alias("detection")
#             )

#             # Check if detection column exists and has expected structure
#             if "detection" not in processed.columns:
#                 logging.info("Missing detection data after explode operation")
#                 return None

#             # Build select expressions with fallbacks
#             select_exprs = [
#                 "frame_number",
#                 F.col("detection.tracker_id").alias("tracker_id"),
#                 coalesce(
#                     F.col("detection.entry_time"),
#                     F.col("detection.exit_time"),
#                     lit(datetime.now(timezone.utc).isoformat())
#                 ).alias("timestamp")
#             ]

#             # Define optional fields with their default values
#             optional_fields = {
#                 "bbox": (F.col("detection.bbox"), array(lit(0.0), lit(0.0), lit(0.0), lit(0.0))),
#                 "speed": (F.col("detection.speed"), lit(0.0)),
#                 "direction": (F.col("detection.direction"), lit("Unknown")),
#                 "lane": (F.col("detection.lane"), lit("Unknown")),
#                 "stopped": (F.col("detection.stopped"), lit(False)),
#                 "confidence": (F.col("detection.confidence"), lit(0.0)),
#                 "class_name": (F.col("detection.class_name"), lit("Unknown")),
#                 "vehicle_color": (F.col("detection.vehicle_color"), lit("Unknown")),
#                 "red_light_violation": (F.col("detection.red_light_violation"), lit(False)),
#                 "line_crossing": (F.col("detection.line_crossing"), lit(False))
#             }

#             # Check which optional fields exist in the schema
#             detection_schema_fields = [f.name for f in processed.schema["detection"].dataType.fields]
            
#             for field_name, (expr, default) in optional_fields.items():
#                 if field_name in detection_schema_fields:
#                     select_exprs.append(expr.alias(field_name))
#                 else:
#                     logging.warning(f"Missing optional field {field_name} in detection data, using default")
#                     select_exprs.append(default.alias(field_name))

#             return processed.select(*select_exprs)

#         except Exception as e:
#             logging.error(f"Error processing vehicle format: {str(e)}", exc_info=True)
#             return None

#     def _group_data(self, df):
#         """Group vehicle detection data by tracker_id and aggregate the new fields"""
#         try:
#             # Validate required columns
#             required_cols = ["tracker_id", "timestamp"]
#             missing_cols = [col for col in required_cols if col not in df.columns]
#             if missing_cols:
#                 logging.info(f"Missing required columns: {missing_cols}")
#                 return None

#             # Base aggregations that should always be present
#             agg_exprs = [
#                 spark_min("timestamp").alias("first_detection"),
#                 spark_max("timestamp").alias("last_detection"),
#                 count("timestamp").alias("frame_count"),
#                 coalesce(avg("confidence"), lit(0.0)).alias("confidence_avg")
#             ]

#             # Define optional aggregations
#             optional_aggs = {
#                 "speed": [
#                     coalesce(avg("speed"), lit(0.0)).alias("avg_speed"),
#                     coalesce(spark_max("speed"), lit(0.0)).alias("max_speed"),
#                     coalesce(spark_min("speed"), lit(0.0)).alias("min_speed"),
#                     collect_list("speed").alias("speed_list")
#                 ],
#                 "class_name": [F.first("class_name", ignorenulls=True).alias("vehicle_type")],
#                 "vehicle_color": [
#                     F.first("vehicle_color", ignorenulls=True).alias("vehicle_color"),
#                     collect_list("vehicle_color").alias("color_list")
#                 ],
#                 "lane": [
#                     F.first("lane", ignorenulls=True).alias("initial_lane"),
#                     F.last("lane", ignorenulls=True).alias("final_lane"),
#                     collect_list("lane").alias("lanes")
#                 ],
#                 "direction": [
#                     F.last("direction", ignorenulls=True).alias("direction"),
#                     collect_list("direction").alias("directions")
#                 ],
#                 "stopped": [collect_list("stopped").alias("stopped_list")],
#                 "bbox": [collect_list("bbox").alias("bbox_list")],
#                 "timestamp": [collect_list("timestamp").alias("timestamps")],
#                 "red_light_violation": [
#                     F.sum(F.expr("CASE WHEN red_light_violation = true THEN 1 ELSE 0 END"))
#                     .alias("red_light_violation_count")
#                 ],
#                 "line_crossing": [
#                     F.sum(F.expr("CASE WHEN line_crossing = true THEN 1 ELSE 0 END"))
#                     .alias("line_crossing_count")
#                 ]
#             }

#             # Add optional aggregations if columns exist
#             for field_name, exprs in optional_aggs.items():
#                 if field_name in df.columns:
#                     agg_exprs.extend(exprs)

#             return df.groupBy("tracker_id").agg(*agg_exprs)

#         except Exception as e:
#             logging.error(f"Error grouping vehicle data: {str(e)}", exc_info=True)
#             return None

#     def _enrich_vehicle(self, row):
#         """Enrich vehicle data with aggregated information and the new counts"""
#         try:
#             # Convert Spark Row to dictionary first
#             row_dict = row.asDict()
            
#             tid = str(row_dict.get("tracker_id", ""))

#             # Safely get list fields with defaults using dictionary access
#             lanes = row_dict.get("lanes", []) or []
#             directions = row_dict.get("directions", []) or []
#             speeds = row_dict.get("speed_list", []) or []
#             bbox_list = row_dict.get("bbox_list", []) or []
#             timestamps = row_dict.get("timestamps", []) or []
#             stopped_list = row_dict.get("stopped_list", []) or []
#             colors = row_dict.get("color_list", []) or []

#             # Calculate most common values with fallbacks
#             lane_counts = Counter(lanes)
#             most_common_lane = lane_counts.most_common(1)[0][0] if lane_counts else row_dict.get("initial_lane", "Unknown")
            
#             direction_counts = Counter(directions)
#             most_common_direction = direction_counts.most_common(1)[0][0] if direction_counts else row_dict.get("direction", "Unknown")
            
#             color_counts = Counter(colors)
#             most_common_color = color_counts.most_common(1)[0][0] if color_counts else row_dict.get("vehicle_color", "Unknown")

#             # Calculate lane and direction changes
#             lane_changes = sum(1 for i in range(1, len(lanes)) if lanes[i] != lanes[i - 1]) if len(lanes) > 1 else 0
#             lane_change_frequency = lane_changes / len(lanes) if lanes else 0
            
#             direction_changes = sum(1 for i in range(1, len(directions)) if directions[i] != directions[i - 1]) if len(directions) > 1 else 0

#             # Calculate stopped duration
#             stopped_duration = 0.0
#             if stopped_list and timestamps and len(stopped_list) == len(timestamps) - 1:
#                 for s, t1, t2 in zip(stopped_list, timestamps[:-1], timestamps[1:]):
#                     if s and t1 and t2:
#                         try:
#                             td = (t2.timestamp() - t1.timestamp())
#                             stopped_duration += td
#                         except:
#                             continue

#             # Calculate direction time distribution
#             direction_time = defaultdict(float)
#             if directions and timestamps and len(directions) == len(timestamps):
#                 for i in range(1, len(directions)):
#                     d1 = directions[i - 1]
#                     t1, t2 = timestamps[i - 1], timestamps[i]
#                     if d1 and t1 and t2:
#                         try:
#                             delta = (t2.timestamp() - t1.timestamp())
#                             direction_time[d1] += delta
#                         except:
#                             continue

#             # Calculate movement metrics
#             movement_angles = []
#             total_distance = 0.0
#             if bbox_list and len(bbox_list) >= 2:
#                 for i in range(1, len(bbox_list)):
#                     try:
#                         b1 = bbox_list[i - 1]
#                         b2 = bbox_list[i]
#                         if None in b1 or None in b2:
#                             continue
                            
#                         x1 = (b1[0] + b1[2]) / 2
#                         y1 = (b1[1] + b1[3]) / 2
#                         x2 = (b2[0] + b2[2]) / 2
#                         y2 = (b2[1] + b2[3]) / 2
#                         dist = math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)
#                         total_distance += dist
#                         angle = math.degrees(math.atan2(y2 - y1, x2 - x1))
#                         movement_angles.append(angle)
#                     except:
#                         continue

#             avg_movement_angle = sum(movement_angles) / len(movement_angles) if movement_angles else 0.0
#             speed_variation = statistics.stdev(speeds) if len(speeds) >= 2 else 0.0

#             # Calculate duration safely
#             try:
#                 duration = (row_dict["last_detection"] - row_dict["first_detection"]).total_seconds()
#             except:
#                 duration = 0.0

#             return tid, {
#                 "vehicle_type": row_dict.get("vehicle_type", "Unknown"),
#                 "vehicle_color": most_common_color,
#                 "confidence_avg": float(row_dict.get("confidence_avg", 0.0)),
#                 "first_detection": row_dict["first_detection"].isoformat() if "first_detection" in row_dict else datetime.now(timezone.utc).isoformat(),
#                 "last_detection": row_dict["last_detection"].isoformat() if "last_detection" in row_dict else datetime.now(timezone.utc).isoformat(),
#                 "duration_seconds": duration,
#                 "stopped_duration": stopped_duration,
#                 "frame_count": row_dict.get("frame_count", 0),
#                 "avg_speed": float(row_dict.get("avg_speed", 0.0)),
#                 "max_speed": float(row_dict.get("max_speed", 0.0)),
#                 "min_speed": float(row_dict.get("min_speed", 0.0)),
#                 "speed_variation": speed_variation,
#                 "lane_changes": lane_changes,
#                 "initial_lane": row_dict.get("initial_lane", "Unknown"),
#                 "final_lane": row_dict.get("final_lane", "Unknown"),
#                 "most_common_lane": most_common_lane,
#                 "lane_change_frequency": lane_change_frequency,
#                 "line_crossing_count": row_dict.get("line_crossing_count", 0),
#                 "direction": most_common_direction,
#                 "direction_changes": direction_changes,
#                 "time_spent_per_direction": dict(direction_time),
#                 "red_light_violation_count": row_dict.get("red_light_violation_count", 0),
#                 "total_distance": total_distance,
#                 "movement_angles": movement_angles,
#                 "avg_movement_angle": avg_movement_angle,
#             }

#         except Exception as e:
#             logging.error(f"Error enriching vehicle data: {str(e)}", exc_info=True)
#             return str(row_dict.get("tracker_id", "unknown")), {"error": f"Failed to enrich vehicle data: {str(e)}"}

import json
import sys
import math
from datetime import datetime, timezone
from pathlib import Path
import statistics
from collections import Counter, defaultdict

sys.path.append(str(Path(__file__).parent.parent))
from ..connectors.minio_connector import MinIOConnector
from pyspark.sql.functions import col, min as spark_min, max as spark_max, avg, stddev, count, collect_list, expr, udf, \
    explode
from pyspark.sql import functions as F


class VehicleProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.minio = MinIOConnector(spark)

    def _process_vehicle_format(self, df):
        """Process vehicle detection format"""
        df = df.select(
            F.col("frame_data.frame_number").alias("frame_number"),
            F.explode("frame_data.detections").alias("detection")
        )

        # Extract the required information
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
            F.col("detection.vehicle_color").alias("vehicle_color"),

            # ADD the new fields you need
            F.col("detection.red_light_violation").alias("red_light_violation"),
            F.col("detection.line_crossing").alias("line_crossing")
            # F.col("detection.geolocation.latitude").alias("latitude"),
            # F.col("detection.geolocation.longitude").alias("longitude")
        ).filter((F.col("tracker_id").isNotNull()) & (F.col("tracker_id") != -1))

        return df

    def _group_data(self, df):
        """Group vehicle detection data by tracker_id and aggregate the new fields"""
        return df.groupBy("tracker_id").agg(
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
            F.collect_list("timestamp").alias("timestamps"),

            # Aggregate the red_light_violation and line_crossing fields
            F.sum(F.expr("CASE WHEN red_light_violation = true THEN 1 ELSE 0 END")).alias("red_light_violation_count"),
            F.sum(F.expr("CASE WHEN line_crossing = true THEN 1 ELSE 0 END")).alias("line_crossing_count")
            # F.first("latitude", ignorenulls=True).alias("latitude"),
            # F.first("longitude", ignorenulls=True).alias("longitude")

        )

    def _enrich_vehicle(self, row):
        """Enrich vehicle data with aggregated information and the new counts"""
        tid = str(row["tracker_id"])
        lanes = row["lanes"] or []
        directions = row["directions"] or []
        speeds = row["speed_list"] or []
        bbox_list = row["bbox_list"] or []
        timestamps = row["timestamps"] or []
        stopped_list = row["stopped_list"] or []
        colors = row["color_list"] or []

        lane_counts = Counter(lanes)
        most_common_lane = lane_counts.most_common(1)[0][0] if lane_counts else "Unknown"
        direction_counts = Counter(directions)
        most_common_direction = direction_counts.most_common(1)[0][0] if direction_counts else "Unknown"
        color_counts = Counter(colors)
        most_common_color = color_counts.most_common(1)[0][0] if color_counts else row["vehicle_color"] or "Unknown"

        lane_changes = sum(1 for i in range(1, len(lanes)) if lanes[i] != lanes[i - 1])
        lane_change_frequency = lane_changes / len(lanes) if lanes else 0
        direction_changes = sum(1 for i in range(1, len(directions)) if directions[i] != directions[i - 1])
        stopped_duration = 0.0

        direction_time = defaultdict(float)
        for i in range(1, len(directions)):
            d1 = directions[i - 1]
            t1, t2 = timestamps[i - 1], timestamps[i]
            if d1 and t1 and t2:
                delta = (t2.timestamp() - t1.timestamp())
                direction_time[d1] += delta

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
                dist = math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)
                total_distance += dist
                angle = math.degrees(math.atan2(y2 - y1, x2 - x1))
                movement_angles.append(angle)
            except:
                continue

        avg_movement_angle = sum(movement_angles) / len(movement_angles) if movement_angles else 0.0
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
            "line_crossing_count": row["line_crossing_count"],
            "direction": most_common_direction,
            "direction_changes": direction_changes,
            "time_spent_per_direction": dict(direction_time),
            "red_light_violation_count": row["red_light_violation_count"],
            # "latitude": row["latitude"],
            # "longitude": row["longitude"],
            "total_distance": total_distance,
            "movement_angles": movement_angles,
            "avg_movement_angle": avg_movement_angle,

        }