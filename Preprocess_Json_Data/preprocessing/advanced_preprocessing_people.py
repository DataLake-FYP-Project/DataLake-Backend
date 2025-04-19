import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))
from connectors.minio_connector import MinIOConnector
from config.spark_config import create_spark_session
from config.minio_config import BUCKETS
from pyspark.sql import functions as F
from pyspark.sql.functions import col, min as spark_min, max as spark_max, avg, count, collect_list


class PeopleProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.minio = MinIOConnector(spark)

    def process_files(self, input_bucket: str, output_bucket: str):
        files = self.minio.list_json_files(input_bucket, "people_detection/")
        for file in files:
            print(f"\nProcessing {file}...")
            try:
                df = self.minio.read_json(input_bucket, f"people_detection/{file}")

                if "frame_detections" in df.columns:
                    df = self._process_frame_detections_format(df)
                elif "detections" in df.columns:
                    df = self._process_flat_detections_format(df)
                else:
                    print(f"Skipping {file} - unknown format")
                    continue

                # Convert timestamps with proper timezone handling
                df = df.withColumn("timestamp", 
                    F.to_timestamp(F.regexp_replace("timestamp", r"\+05:30$", ""))
                )
                
                if "frame_timestamp" in df.columns:
                    df = df.withColumn("frame_timestamp", 
                        F.to_timestamp(F.regexp_replace("frame_timestamp", r"\+05:30$", ""))
                    )

                # Group by tracker_id and collect all relevant information
                grouped = df.groupBy("tracker_id").agg(
                    spark_min("timestamp").alias("first_detection"),
                    spark_max("timestamp").alias("last_detection"),
                    count("timestamp").alias("frame_count"),
                    avg("confidence").alias("confidence_avg"),
                    collect_list("age").alias("age_list"),
                    collect_list("gender").alias("gender_list"),
                    collect_list("carrying").alias("carrying_list"),
                    collect_list("mask_status").alias("mask_status_list"),
                    avg("mask_confidence").alias("mask_confidence_avg"),
                    collect_list("in_restricted_area").alias("restricted_area_list"),
                    collect_list(col("timestamp").alias("restricted_timestamps")).alias("restricted_timestamps"),
                    collect_list("bbox").alias("bbox_list"),
                    collect_list("frame_number").alias("frame_numbers"),
                    collect_list("confidence").alias("confidence_list")
                )

                enriched_data = dict(sorted([self._enrich_person(row) for row in grouped.collect()], 
                                         key=lambda x: int(x[0])))

                output = {
                    "source_file": file,
                    "processing_date": datetime.now(timezone.utc).isoformat(),
                    "people_count": len(enriched_data),
                    "people": enriched_data
                }

                out_path = f"refine_{file}"
                self.minio.write_single_json(output, output_bucket, out_path)
                print(f"Successfully processed {len(enriched_data)} people in {file}")

            except Exception as e:
                print(f"Error processing file {file}: {str(e)}")
                continue

    def _process_frame_detections_format(self, df):
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
            F.col("detection.mask_status").alias("mask_status"),
            F.col("detection.mask_confidence").alias("mask_confidence"),
            F.col("detection.in_restricted_area").alias("in_restricted_area"),
            F.col("detection.confidence").alias("confidence")
        ).filter(F.col("tracker_id").isNotNull())

    def _process_flat_detections_format(self, df):
        # Get all the field names from the detections struct
        detection_fields = [f.name for f in df.schema["detections"].dataType.fields]
        
        # Create an array of all detection structs
        detections_expr = F.array([F.col(f"detections.{field}") for field in detection_fields])
        
        # Explode the array to get one row per detection
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
            F.coalesce(
                F.col("detection.mask_status"),
                F.lit("unknown")
            ).alias("mask_status"),
            F.coalesce(
                F.col("detection.mask_confidence"),
                F.lit(0.0)
            ).alias("mask_confidence"),
            F.coalesce(
                F.col("detection.in_restricted_area"),
                F.col("detection.entered_restricted"),
                F.lit(False)
            ).alias("in_restricted_area"),
            F.col("detection.confidence").alias("confidence")
        ).filter(
            F.col("detection").isNotNull() & 
            F.col("tracker_id").isNotNull()
        )

    def _enrich_person(self, row):
        tid = str(row["tracker_id"])
        
        # Get most frequent values for categorical attributes
        def get_most_frequent(lst):
            if not lst:
                return "Unknown"
            lst = [x for x in lst if x and x != "Unknown"]
            if not lst:
                return "Unknown"
            return Counter(lst).most_common(1)[0][0]

        age = get_most_frequent(row["age_list"])
        gender = get_most_frequent(row["gender_list"])
        mask_status = get_most_frequent(row["mask_status_list"])
        carrying = get_most_frequent(row["carrying_list"])

        # Check if person entered restricted area and get first entry time
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
            "mask_status": mask_status,
            "mask_confidence_avg": float(row["mask_confidence_avg"] or 0.0),
            "entered_restricted_area": restricted_entry_time is not None,
            "restricted_area_entry_time": restricted_entry_time.isoformat() if restricted_entry_time else None,
            "first_detection": row["first_detection"].isoformat(),
            "last_detection": row["last_detection"].isoformat(),
            "duration_seconds": float((row["last_detection"] - row["first_detection"]).total_seconds()),
            "frame_count": int(row["frame_count"])
        }

if __name__ == '__main__':
    spark = create_spark_session()
    processor = PeopleProcessor(spark)
    processor.process_files(BUCKETS["processed"], BUCKETS["refine"])
    spark.stop()