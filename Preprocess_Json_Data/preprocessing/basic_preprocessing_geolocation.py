from pyspark.sql import DataFrame
from pyspark.sql.functions import col, struct, lit
from pyspark.sql.types import DoubleType, ArrayType
import logging

def process_geolocation_json_data(df: DataFrame):
    try:
        if df is None or len(df.columns) == 0:
            logging.warning("Input DataFrame is empty or invalid.")
            return df, -1

        logging.info(f"Initial columns in DataFrame: {df.columns}")

        # Rename 'frame' to 'frame_number' if present
        if "frame" in df.columns and "frame_number" not in df.columns:
            df = df.withColumnRenamed("frame", "frame_number")

        # Add bbox_x1, bbox_y1, bbox_x2, bbox_y2 if bbox column exists
        if "bbox" in df.columns:
            df = df.withColumn("bbox", col("bbox").cast(ArrayType(DoubleType())))
            for i, field in enumerate(["x1", "y1", "x2", "y2"]):
                df = df.withColumn(f"bbox_{field}", col("bbox")[i])

        # Extract geolocation if present
        if "geolocation" in df.columns:
            if "geolocation.latitude" in df.columns or "geolocation" in df.schema.fieldNames():
                df = df.withColumn("geolocation_latitude", col("geolocation.latitude").cast(DoubleType()))
                df = df.withColumn("geolocation_longitude", col("geolocation.longitude").cast(DoubleType()))

        # Fill missing columns with defaults for downstream compatibility
        defaults = {
            "confidence": 0.0,
            "class_id": -1,
            "bbox_x1": 0.0,
            "bbox_y1": 0.0,
            "bbox_x2": 0.0,
            "bbox_y2": 0.0,
            "geolocation_latitude": 0.0,
            "geolocation_longitude": 0.0
        }

        for field, default in defaults.items():
            if field not in df.columns:
                df = df.withColumn(field, lit(default))

        # Optional: filter low-confidence detections if confidence exists
        if "confidence" in df.columns:
            df = df.filter(col("confidence") > 0.1)

        # Reconstruct detection struct if desired
        detection_fields = []
        if "class" in df.columns:
            detection_fields.append(col("class"))
        if "class_id" in df.columns:
            detection_fields.append(col("class_id"))
        if "confidence" in df.columns:
            detection_fields.append(col("confidence"))
        if "bbox" in df.columns:
            detection_fields.append(col("bbox"))
        if "geolocation_latitude" in df.columns and "geolocation_longitude" in df.columns:
            detection_fields.append(
                struct(
                    col("geolocation_latitude").alias("latitude"),
                    col("geolocation_longitude").alias("longitude")
                ).alias("geolocation")
            )

        if detection_fields:
            df = df.withColumn("detection", struct(*detection_fields))

        return df, 1

    except Exception as e:
        logging.error(f"Error processing geolocation detection data: {str(e)}")
        return df, -1
