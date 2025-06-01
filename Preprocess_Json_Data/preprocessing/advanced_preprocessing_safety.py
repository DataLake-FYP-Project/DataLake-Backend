import json
import sys
import math
from datetime import datetime, timezone
from pathlib import Path
from collections import Counter, defaultdict

from pyspark.sql.functions import col, explode, coalesce, lit, first, collect_list, count, min as spark_min, max as spark_max
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).parent.parent))
from ..connectors.minio_connector import MinIOConnector


class SafetyProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.minio = MinIOConnector(spark)

    def _process_safety_format(self, df):
        df = df.select(
            F.col("frame_data.frame_number").alias("frame_number"),
            F.explode("frame_data.people").alias("person")
        )
        df = df.select(
            "frame_number",
            F.col("person.tracker_id").alias("tracker_id"),
            F.col("person.bbox").alias("bbox"),
            coalesce(
                F.lit(datetime.now(timezone.utc).isoformat())
            ).alias("timestamp"),
            F.col("person.hardhat").alias("hardhat"),
            F.col("person.mask").alias("mask"),
            F.col("person.safety_vest").alias("safety_vest"),
            F.col("person.safety_status").alias("safety_status"),
            F.col("person.missing_items").alias("missing_items")
        ).filter((F.col("tracker_id").isNotNull()) & (F.col("tracker_id") != -1))
        return df

    def _group_data(self, df):
        return df.groupBy("tracker_id").agg(
            spark_min("frame_number").alias("start_frame"),
            spark_max("frame_number").alias("end_frame"),
            count("frame_number").alias("frame_count"),
            first("safety_status", ignorenulls=True).alias("initial_status"),
            collect_list("safety_status").alias("status_list"),
            collect_list("hardhat").alias("hardhat_list"),
            collect_list("mask").alias("mask_list"),
            collect_list("safety_vest").alias("safety_vest_list"),
            collect_list("missing_items").alias("missing_items_list"),
            collect_list("bbox").alias("bbox_list")
        )

    def _enrich_safety(self, row):
        tid = str(row["tracker_id"])
        hardhat_list = row["hardhat_list"] or []
        mask_list = row["mask_list"] or []
        vest_list = row["safety_vest_list"] or []
        missing_items_list = row["missing_items_list"] or []
        status_list = row["status_list"] or []
        bbox_list = row["bbox_list"] or []

        # Compute violations
        hardhat_violations = sum(1 for v in hardhat_list if v is False)
        mask_violations = sum(1 for v in mask_list if v is False or v == "false")
        vest_violations = sum(1 for v in vest_list if v is False or v == "false")
        total_unsafe = status_list.count("Unsafe")

        # Missing item analysis
        all_missing = [item for sublist in missing_items_list if sublist for item in sublist]
        missing_counter = Counter(all_missing)

        # Safety condition resolution
        final_safety_status = "Safe" if "Safe" in status_list else "Unsafe"
        final_hardhat = any(h is True for h in hardhat_list)
        final_mask = any(m is True for m in mask_list)
        final_vest = any(v is True for v in vest_list)

        # Duration
        duration = row["frame_count"]

        return tid, {
            "initial_safety_status": row["initial_status"],
            "safety_status": final_safety_status,
            "hardhat": final_hardhat,
            "mask": final_mask,
            "safety_vest": final_vest,
            "total_frames": row["frame_count"],
            "duration_frames": duration,
            "hardhat_violations": hardhat_violations,
            "mask_violations": mask_violations,
            "safety_vest_violations": vest_violations,
            "total_unsafe_frames": total_unsafe,
            "most_common_missing_item": missing_counter.most_common(1)[0][0] if missing_counter else None,
            "bbox_count": len(bbox_list),
            "bbox_movement_estimate": self._estimate_movement(bbox_list)
        }


    def _estimate_movement(self, bbox_list):
        total_distance = 0.0
        for i in range(1, len(bbox_list)):
            try:
                b1 = bbox_list[i - 1]
                b2 = bbox_list[i]
                x1, y1 = (b1[0] + b1[2]) / 2, (b1[1] + b1[3]) / 2
                x2, y2 = (b2[0] + b2[2]) / 2, (b2[1] + b2[3]) / 2
                total_distance += math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)
            except:
                continue
        return total_distance

    def generate_final_output(self, grouped_df, source_filename: str):
        """Generate final structured JSON output"""
        enriched_data = grouped_df.rdd.map(self._enrich_safety).collectAsMap()

        return {
            "source_file": source_filename,
            "processing_date": datetime.now(timezone.utc).isoformat(),
            "processing_version": "1.0",
            "safety_count": len(enriched_data),
            "safety_objects": enriched_data
        }
