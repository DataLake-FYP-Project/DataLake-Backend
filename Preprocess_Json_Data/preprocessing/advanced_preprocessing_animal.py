from collections import Counter
from pathlib import Path
import sys
import logging

sys.path.append(str(Path(__file__).parent.parent))
from ..connectors.minio_connector import MinIOConnector
from pyspark.sql import functions as F
from pyspark.sql.functions import col, min as spark_min, max as spark_max, avg, count, collect_list

class AnimalProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.minio = MinIOConnector(spark)

    def _group_data(self, df):
        """Group animal detection data by class_name and bbox similarity"""
        # Create a unique identifier based on class_name and approximate bbox position
        df = df.withColumn(
            "animal_id",
            F.concat(
                F.col("class_name"),
                F.lit("_"),
                F.floor(F.col("bbox.center_x")/10).cast("int"),
                F.lit("_"),
                F.floor(F.col("bbox.center_y")/10).cast("int")
            )
        )
        
        return df.groupBy("animal_id").agg(
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
        """Process frame detections format"""
        # Process the frame_data.detections format
        processed_df = df.select(
            F.col("frame_data.frame_number").alias("frame_number"),
            F.col("frame_data.timestamp").alias("timestamp"),
            F.explode("frame_data.detections").alias("detection")
        ).select(
            "frame_number",
            "timestamp",
            F.col("detection.bbox").alias("bbox"),
            F.col("detection.class_id").alias("class_id"),
            F.col("detection.class_name").alias("class_name"),
            F.col("detection.confidence").alias("confidence")
        )
        
        # Cast bbox fields to float and add center coordinates for grouping
        processed_df = processed_df.withColumn(
            "bbox.center_x",
            (F.col("bbox.bbox_x1").cast("float") + F.col("bbox.bbox_x2").cast("float")) / 2
        ).withColumn(
            "bbox.center_y",
            (F.col("bbox.bbox_y1").cast("float") + F.col("bbox.bbox_y2").cast("float")) / 2
        )
        
        return processed_df

    def _enrich_animal(self, row):
        """Enrich animal data with aggregated information"""
        animal_id = str(row["animal_id"])

        return animal_id, {
            "class_name": row["class_name"],
            "confidence_avg": float(row["confidence_avg"] or 0.0),
            "first_detection": row["first_detection"],
            "last_detection": row["last_detection"],
            "duration_seconds": float((row["last_detection"] - row["first_detection"])),
            "frame_count": int(row["frame_count"]),
            "bbox_list": row["bbox_list"],
            "frame_numbers": row["frame_numbers"]
        }
