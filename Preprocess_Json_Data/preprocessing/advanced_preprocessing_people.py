# from collections import Counter
# from pathlib import Path
# import sys
# import logging
# from datetime import datetime, timezone

# sys.path.append(str(Path(__file__).parent.parent))
# from ..connectors.minio_connector import MinIOConnector
# from pyspark.sql import functions as F
# from pyspark.sql.functions import col, min as spark_min, max as spark_max, avg, count, collect_list, lit, coalesce, array


# class PeopleProcessor:
#     def __init__(self, spark):
#         self.spark = spark
#         self.minio = MinIOConnector(spark)

#     def _group_data(self, df):
#         """Group people detection data with robust error handling"""
#         try:
#             # Validate required columns
#             required_cols = ["tracker_id", "timestamp"]
#             missing_cols = [col for col in required_cols if col not in df.columns]
#             if missing_cols:
#                 logging.info(f"Missing required columns: {missing_cols}")
#                 return None

#             # Base aggregations
#             agg_exprs = [
#                 spark_min("timestamp").alias("first_detection"),
#                 spark_max("timestamp").alias("last_detection"),
#                 count("timestamp").alias("frame_count"),
#                 coalesce(avg("confidence"), lit(0.0)).alias("confidence_avg")
#             ]

#             # Define optional aggregations
#             optional_aggs = {
#                 "age": [collect_list("age").alias("age_list")],
#                 "gender": [collect_list("gender").alias("gender_list")],
#                 "carrying": [collect_list("carrying").alias("carrying_list")],
#                 "in_restricted_area": [
#                     collect_list("in_restricted_area").alias("restricted_area_list"),
#                     collect_list("timestamp").alias("restricted_timestamps")
#                 ],
#                 "bbox": [collect_list("bbox").alias("bbox_list")],
#                 "frame_number": [collect_list("frame_number").alias("frame_numbers")],
#                 "confidence": [collect_list("confidence").alias("confidence_list")]
#             }

#             # Add optional aggregations if columns exist
#             for field_name, exprs in optional_aggs.items():
#                 if field_name in df.columns:
#                     agg_exprs.extend(exprs)

#             return df.groupBy("tracker_id").agg(*agg_exprs)

#         except Exception as e:
#             logging.error(f"Error grouping people data: {str(e)}", exc_info=True)
#             return None

#     def _process_frame_detections_format(self, df):
#         """Process frame detections format with robust error handling"""
#         try:
#             if "frame_detections" not in df.columns:
#                 logging.info("Missing required column: frame_detections")
#                 return None

#             # First level explode
#             exploded = df.select(F.explode("frame_detections").alias("frame_detection"))
            
#             if "frame_detection" not in exploded.columns:
#                 logging.info("Failed to explode frame_detections")
#                 return None

#             # Second level processing
#             result = exploded.select(
#                 F.col("frame_detection.frame_number").alias("frame_number"),
#                 F.col("frame_detection.timestamp").alias("frame_timestamp"),
#                 F.explode("frame_detection.detections").alias("detection")
#             )

#             if "detection" not in result.columns:
#                 logging.info("Failed to explode detections")
#                 return None

#             # Get available detection fields
#             detection_fields = [f.name for f in result.schema["detection"].dataType.fields]

#             # Build select expressions with fallbacks
#             select_exprs = [
#                 "frame_number",
#                 "frame_timestamp",
#                 F.col("detection.tracker_id").alias("tracker_id"),
#                 coalesce(
#                     F.col("detection.entry_time"),
#                     F.col("detection.exit_time"),
#                     F.col("frame_timestamp")
#                 ).alias("timestamp")
#             ]

#             # Define optional fields with defaults
#             optional_fields = {
#                 "bbox": (F.col("detection.bbox"), array(lit(0.0), lit(0.0), lit(0.0), lit(0.0))),
#                 "age": (F.col("detection.age"), lit("Unknown")),
#                 "gender": (F.col("detection.gender"), lit("Unknown")),
#                 "carrying": (F.col("detection.carrying"), lit("Unknown")),
#                 "in_restricted_area": (F.col("detection.in_restricted_area"), lit(False)),
#                 "confidence": (F.col("detection.confidence"), lit(0.0))
#             }

#             for field_name, (expr, default) in optional_fields.items():
#                 if field_name in detection_fields:
#                     select_exprs.append(expr.alias(field_name))
#                 else:
#                     logging.warning(f"Missing optional field {field_name} in detection data, using default")
#                     select_exprs.append(default.alias(field_name))

#             return result.select(*select_exprs).filter(F.col("tracker_id").isNotNull())

#         except Exception as e:
#             logging.error(f"Error processing frame detections: {str(e)}", exc_info=True)
#             return None

#     def _process_flat_detections_format(self, df):
#         """Process flat detections format"""
#         try:
#             if "detections" not in df.columns:
#                 logging.info("Missing required column: detections")
#                 return None

#             # Get available detection fields
#             detection_fields = [f.name for f in df.schema["detections"].dataType.fields]

#             # Build array expression for explode
#             available_fields = []
#             field_mapping = {
#                 "tracker_id": "tracker_id",
#                 "entry_time": "entry_time",
#                 "exit_time": "exit_time",
#                 "bbox_x1": "bbox_x1",
#                 "bbox_y1": "bbox_y1",
#                 "bbox_x2": "bbox_x2",
#                 "bbox_y2": "bbox_y2",
#                 "age": "age",
#                 "gender": "gender",
#                 "carrying": "carrying",
#                 "in_restricted_area": "in_restricted_area",
#                 "entered_restricted": "entered_restricted",
#                 "confidence": "confidence"
#             }

#             # Create struct with available fields
#             struct_fields = []
#             for src, dest in field_mapping.items():
#                 if src in detection_fields:
#                     struct_fields.append(F.col(f"detections.{src}").alias(dest))

#             if not struct_fields:
#                 logging.info("No valid detection fields found")
#                 return None

#             detections_expr = F.array(F.struct(*struct_fields))

#             # Explode and process
#             exploded = df.select(F.explode(detections_expr).alias("detection"))

#             # Build final select with fallbacks
#             select_exprs = [
#                 lit(None).cast("integer").alias("frame_number"),
#                 lit(None).cast("timestamp").alias("frame_timestamp"),
#                 F.col("detection.tracker_id").alias("tracker_id"),
#                 coalesce(
#                     F.col("detection.entry_time"),
#                     F.col("detection.exit_time"),
#                     F.current_timestamp()
#                 ).alias("timestamp")
#             ]

#             # Handle bbox fields
#             if all(f"bbox_{coord}" in [f.name for f in exploded.schema["detection"].dataType.fields] for coord in ["x1", "y1", "x2", "y2"]):
#                 select_exprs.append(
#                     array(
#                         F.col("detection.bbox_x1"),
#                         F.col("detection.bbox_y1"),
#                         F.col("detection.bbox_x2"),
#                         F.col("detection.bbox_y2")
#                     ).alias("bbox")
#                 )
#             else:
#                 logging.warning("Missing bbox coordinates, using default")
#                 select_exprs.append(array(lit(0.0), lit(0.0), lit(0.0), lit(0.0)).alias("bbox"))

#             # Add other optional fields
#             optional_fields = {
#                 "age": (F.col("detection.age"), lit("Unknown")),
#                 "gender": (F.col("detection.gender"), lit("Unknown")),
#                 "carrying": (F.col("detection.carrying"), lit("Unknown")),
#                 "in_restricted_area": (
#                     coalesce(
#                         F.col("detection.in_restricted_area"),
#                         F.col("detection.entered_restricted"),
#                         lit(False)
#                     ),
#                     lit(False)
#                 ),
#                 "confidence": (F.col("detection.confidence"), lit(0.0))
#             }

#             for field_name, (expr, default) in optional_fields.items():
#                 if field_name in [f.name for f in exploded.schema["detection"].dataType.fields]:
#                     select_exprs.append(expr.alias(field_name))
#                 else:
#                     logging.warning(f"Missing optional field {field_name}, using default")
#                     select_exprs.append(default.alias(field_name))

#             return exploded.select(*select_exprs).filter(
#                 F.col("detection").isNotNull() &
#                 F.col("tracker_id").isNotNull()
#             )

#         except Exception as e:
#             logging.error(f"Error processing flat detections: {str(e)}", exc_info=True)
#             return None

#     def _enrich_person(self, row):
#         """Enrich person data with aggregated information"""
#         try:
#             # Convert Spark Row to dictionary first
#             row_dict = row.asDict()
#             tid = str(row_dict.get("tracker_id", ""))

#             def get_most_frequent(lst):
#                 if not lst:
#                     return "Unknown"
#                 lst = [x for x in lst if x and x != "Unknown"]
#                 if not lst:
#                     return "Unknown"
#                 try:
#                     return Counter(lst).most_common(1)[0][0]
#                 except:
#                     return "Unknown"

#             # Safely get list fields with defaults
#             age_list = row_dict.get("age_list", []) or []
#             gender_list = row_dict.get("gender_list", []) or []
#             carrying_list = row_dict.get("carrying_list", []) or []
#             restricted_areas = row_dict.get("restricted_area_list", []) or []
#             restricted_timestamps = row_dict.get("restricted_timestamps", []) or []

#             # Find restricted area entry time
#             restricted_entry_time = None
#             for i, is_restricted in enumerate(restricted_areas):
#                 if is_restricted and i < len(restricted_timestamps):
#                     try:
#                         restricted_entry_time = restricted_timestamps[i]
#                         break
#                     except:
#                         continue

#             # Calculate duration safely
#             try:
#                 first_detection = row_dict.get("first_detection")
#                 last_detection = row_dict.get("last_detection")
#                 if first_detection and last_detection:
#                     duration = (last_detection - first_detection).total_seconds()
#                 else:
#                     duration = 0.0
#             except:
#                 duration = 0.0

#             return tid, {
#                 "age": get_most_frequent(age_list),
#                 "gender": get_most_frequent(gender_list),
#                 "carrying": get_most_frequent(carrying_list),
#                 "confidence_avg": float(row_dict.get("confidence_avg", 0.0)),
#                 "entered_restricted_area": restricted_entry_time is not None,
#                 "restricted_area_entry_time": restricted_entry_time.isoformat() if restricted_entry_time else None,
#                 "first_detection": first_detection.isoformat() if first_detection else datetime.now(timezone.utc).isoformat(),
#                 "last_detection": last_detection.isoformat() if last_detection else datetime.now(timezone.utc).isoformat(),
#                 "duration_seconds": float(duration),
#                 "frame_count": int(row_dict.get("frame_count", 0))
#             }

#         except Exception as e:
#             logging.error(f"Error enriching person data: {str(e)}", exc_info=True)
#             return "unknown", {"error": f"Failed to enrich person data: {str(e)}"}

from collections import Counter
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))
from ..connectors.minio_connector import MinIOConnector
from pyspark.sql import functions as F
from pyspark.sql.functions import col, min as spark_min, max as spark_max, avg, count, collect_list


class PeopleProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.minio = MinIOConnector(spark)

    def _group_data(self, df):
        """Group people detection data by tracker_id"""
        return df.groupBy("tracker_id").agg(
            spark_min("timestamp").alias("first_detection"),
            spark_max("timestamp").alias("last_detection"),
            count("timestamp").alias("frame_count"),
            avg("confidence").alias("confidence_avg"),
            collect_list("age").alias("age_list"),
            collect_list("gender").alias("gender_list"),
            collect_list("carrying").alias("carrying_list"),
            # collect_list("mask_status").alias("mask_status_list"),
            # avg("mask_confidence").alias("mask_confidence_avg"),
            collect_list("in_restricted_area").alias("restricted_area_list"),
            collect_list(col("timestamp").alias("restricted_timestamps")).alias("restricted_timestamps"),
            collect_list("bbox").alias("bbox_list"),
            collect_list("frame_number").alias("frame_numbers"),
            collect_list("confidence").alias("confidence_list")
        )

    def _process_frame_detections_format(self, df):
        """Process frame detections format"""
        return df.select(
            F.explode("frame_detections").alias("frame_detection")
        ).select(
            F.col("frame_detection.frame_number").alias("frame_number"),
            F.col("frame_detection.timestamp").alias("frame_timestamp"),
            F.explode("frame_detection.detections").alias("detection")
        ).select(
            "frame_number",
            "frame_timestamp",
            F.col("detection.tracker_id").alias("tracker_id"),
            F.coalesce(
                F.col("detection.entry_time"),
                F.col("detection.exit_time"),
                F.col("frame_timestamp")
            ).alias("timestamp"),
            F.col("detection.bbox").alias("bbox"),
            F.col("detection.age").alias("age"),
            F.col("detection.gender").alias("gender"),
            F.col("detection.carrying").alias("carrying"),
            # F.col("detection.mask_status").alias("mask_status"),
            # F.col("detection.mask_confidence").alias("mask_confidence"),
            F.col("detection.in_restricted_area").alias("in_restricted_area"),
            F.col("detection.confidence").alias("confidence")
        ).filter((F.col("tracker_id").isNotNull()) & (F.col("tracker_id") != -1))

    def _process_flat_detections_format(self, df):
        """Process flat detections format"""
        detection_fields = [f.name for f in df.schema["detections"].dataType.fields]
        detections_expr = F.array([F.col(f"detections.{field}") for field in detection_fields])

        return df.select(
            F.explode(detections_expr).alias("detection")
        ).select(
            F.lit(None).cast("integer").alias("frame_number"),
            F.lit(None).cast("timestamp").alias("frame_timestamp"),
            F.col("detection.tracker_id").alias("tracker_id"),
            F.coalesce(
                F.col("detection.entry_time"),
                F.col("detection.exit_time"),
                F.current_timestamp()
            ).alias("timestamp"),
            F.array(
                F.col("detection.bbox_x1"),
                F.col("detection.bbox_y1"),
                F.col("detection.bbox_x2"),
                F.col("detection.bbox_y2")
            ).alias("bbox"),
            F.col("detection.age").alias("age"),
            F.col("detection.gender").alias("gender"),
            F.col("detection.carrying").alias("carrying"),
            # F.coalesce(
            #   F.col("detection.mask_status"),
            #   F.lit("unknown")
            # ).alias("mask_status"),
            # F.coalesce(
            #  F.col("detection.mask_confidence"),
            #    F.lit(0.0)
            # ).alias("mask_confidence"),
            F.coalesce(
                F.col("detection.in_restricted_area"),
                F.col("detection.entered_restricted"),
                F.lit(False)
            ).alias("in_restricted_area"),
            F.col("detection.confidence").alias("confidence")
        ).filter(
            (F.col("detection").isNotNull()) &
            (F.col("tracker_id").isNotNull()) &
            (F.col("tracker_id") != -1)
        )
    

    def _enrich_person(self, row):
        """Enrich person data with aggregated information"""
        
        tid = str(row["tracker_id"])

        def get_most_frequent(lst):
            if not lst:
                return "Unknown"
            lst = [x for x in lst if x and x != "Unknown"]
            if not lst:
                return "Unknown"
            return Counter(lst).most_common(1)[0][0]

        age = get_most_frequent(row["age_list"])
        gender = get_most_frequent(row["gender_list"])
        # mask_status = get_most_frequent(row["mask_status_list"])
        carrying = get_most_frequent(row["carrying_list"])

        restricted_entry_time = None
        restricted_areas = row["restricted_area_list"] or []
        restricted_timestamps = row["restricted_timestamps"] or []

        for i, is_restricted in enumerate(restricted_areas):
            if is_restricted and i < len(restricted_timestamps):
                restricted_entry_time = restricted_timestamps[i]
                break

        return tid, {
            "age": age,
            "gender": gender,
            "carrying": carrying,
            "confidence_avg": float(row["confidence_avg"] or 0.0),
            # "mask_status": mask_status,
            # "mask_confidence_avg": float(row["mask_confidence_avg"] or 0.0),
            "entered_restricted_area": restricted_entry_time is not None,
            "restricted_area_entry_time": restricted_entry_time.isoformat() if restricted_entry_time else None,
            "first_detection": row["first_detection"].isoformat(),
            "last_detection": row["last_detection"].isoformat(),
            "duration_seconds": float((row["last_detection"] - row["first_detection"]).total_seconds()),
            "frame_count": int(row["frame_count"])
        }