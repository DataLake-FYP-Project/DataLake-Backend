from collections import defaultdict
from pathlib import Path
import sys
import logging

from pyspark.sql import functions as F
from pyspark.sql.functions import col, min as spark_min, max as spark_max, avg, count, collect_list, first

sys.path.append(str(Path(__file__).parent.parent))
from ..connectors.minio_connector import MinIOConnector

class CommonProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.minio = MinIOConnector(spark)

    def _group_data(self, df):
        """Group common detection data by class_name and bbox similarity"""
        df = df.withColumn(
            "object_id",
            F.concat(
                F.col("class_name"),
                F.lit("_"),
                F.floor(F.col("bbox.center_x") / 10).cast("int"),
                F.lit("_"),
                F.floor(F.col("bbox.center_y") / 10).cast("int")
            )
        )

        df.groupBy("object_id").agg(
            spark_min("timestamp").alias("first_detection"),
            spark_max("timestamp").alias("last_detection"),
            count("timestamp").alias("frame_count"),
            avg("confidence").alias("confidence_avg"),
            collect_list("class_name").alias("class_name_list"),
            collect_list("bbox").alias("bbox_list"),
            collect_list("frame_number").alias("frame_numbers"),
            collect_list("confidence").alias("confidence_list"),
            F.first("class_name").alias("class_name")
        )

    def _process_frame_detections_format(self, df):
        """Process frame_data format for common detections"""
        processed_df = df.select(
            F.col("frame_data.frame_number").alias("frame_number"),
            F.col("frame_data.timestamp").alias("timestamp"),
            F.explode("frame_data.objects").alias("object")
        ).select(
            "frame_number",
            "timestamp",
            F.col("object.bbox").alias("bbox"),
            F.col("object.class_id").alias("class_id"),
            F.col("object.class_name").alias("class_name"),
            F.col("object.confidence").alias("confidence")
        )

        processed_df = processed_df.withColumn(
            "bbox.center_x",
            (F.col("bbox.bbox_x1").cast("float") + F.col("bbox.bbox_x2").cast("float")) / 2
        ).withColumn(
            "bbox.center_y",
            (F.col("bbox.bbox_y1").cast("float") + F.col("bbox.bbox_y2").cast("float")) / 2
        )

        return processed_df

    def _enrich_object(self, row):
        """Enrich common object with aggregated info"""
        object_id = str(row["object_id"])

        return object_id, {
            "class_name": row["class_name"],
            "confidence_avg": float(row["confidence_avg"] or 0.0),
            "first_detection": row["first_detection"],
            "last_detection": row["last_detection"],
            "duration_seconds": float((row["last_detection"] - row["first_detection"])),
            "frame_count": int(row["frame_count"]),
            "bbox_list": row["bbox_list"],
            "frame_numbers": row["frame_numbers"]
        }
