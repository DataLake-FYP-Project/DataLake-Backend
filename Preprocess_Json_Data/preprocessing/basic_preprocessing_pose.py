from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, struct, lit, array
from pyspark.sql.types import DoubleType, ArrayType, StringType, StructType, StructField
import logging


def process_pose_json_data(df: DataFrame):
    try:
        if df is None or len(df.columns) == 0:
            logging.warning("Input DataFrame is empty or invalid.")
            return df, -1

        logging.info(f"Initial columns in DataFrame: {df.columns}")

        # Ensure 'frame_number' exists (rename 'frame' if present)
        if "frame" in df.columns and "frame_number" not in df.columns:
            df = df.withColumnRenamed("frame", "frame_number")

        # Explode pose_data array to handle multiple detections per frame (though your data has at most one)
        if "pose_data" in df.columns:
            df = df.withColumn("pose_data", explode(col("pose_data"))) \
                .select("frame_number", "pose_data.*")

            # Extract keypoints, action, and confidence
            if "keypoints" in df.columns:
                df = df.withColumn("keypoints", col("keypoints").cast(ArrayType(
                    StructType([
                        StructField("landmark_id", DoubleType()),
                        StructField("x", DoubleType()),
                        StructField("y", DoubleType()),
                        StructField("z", DoubleType()),
                        StructField("visibility", DoubleType())
                    ])
                )))
            if "action" in df.columns:
                df = df.withColumn("action", col("action").cast(StringType()))
            if "confidence" in df.columns:
                df = df.withColumn("confidence", col("confidence").cast(DoubleType()))

        # Fill missing columns with defaults for downstream compatibility
        defaults = {
            "confidence": 0.0,
            "action": "Unknown",
            "keypoints": array()  # Empty array as default for keypoints
        }

        for field, default in defaults.items():
            if field not in df.columns:
                df = df.withColumn(field, lit(default))

        # Optional: filter low-confidence detections
        if "confidence" in df.columns:
            df = df.filter(col("confidence") > 0.1)

        # Create pose_detection struct
        pose_fields = []
        if "action" in df.columns:
            pose_fields.append(col("action"))
        if "confidence" in df.columns:
            pose_fields.append(col("confidence"))
        if "keypoints" in df.columns:
            pose_fields.append(col("keypoints"))

        if pose_fields:
            df = df.withColumn("pose_detection", struct(*pose_fields))

        return df, 1

    except Exception as e:
        logging.error(f"Error processing pose detection data: {str(e)}")
        return df, -1
