# preprocessing/explode_json.py
from pyspark.sql.functions import explode
import logging

def explode_detections(df):
    try:
        if "detections" not in df.columns:
            raise KeyError("Missing 'detections' column in input DataFrame")
        df = df.withColumn("detection", explode("detections"))

        # Handle corrupt records if present
        if "_corrupt_record" in df.columns:
            df = df.filter(df["_corrupt_record"].isNull()).drop("_corrupt_record")
            logging.info("Removed corrupt records from exploded DataFrame.")

        return df
    except Exception as e:
        logging.error(f"Error during explode operation: {e}")
        raise


# preprocessing/filter_confidence.py
from pyspark.sql.functions import col
import logging

def filter_low_confidence(df, threshold=0.7):
    try:
        conf_col = "detection.confidence" if "detection.confidence" in df.columns else "confidence"
        if conf_col not in df.columns:
            raise KeyError("Missing 'confidence' field in detection")

        filtered_df = df.filter(col(conf_col) > threshold)
        logging.info(f"Filtered low-confidence records below threshold: {threshold}")
        return filtered_df
    except Exception as e:
        logging.error(f"Error filtering low-confidence records: {e}")
        raise


# preprocessing/clean_fields.py
from pyspark.sql.functions import col, lit, coalesce
import logging

def clean_and_fill_fields(df):
    try:
        required = ["tracker_id", "class_id", "confidence"]
        for field in required:
            full_col = f"detection.{field}" if f"detection.{field}" in df.columns else field
            if full_col not in df.columns:
                raise KeyError(f"Missing required field: {full_col}")

        cleaned_df = df.select(
            col("frame_number"),
            col("detection.tracker_id").alias("tracker_id"),
            col("detection.class_id").alias("class_id"),
            col("detection.class_name").alias("class_name"),
            col("detection.confidence").alias("confidence"),
            col("detection.direction").alias("direction"),
            col("detection.lane").alias("lane"),
            col("detection.vehicle_color").alias("vehicle_color"),
            col("detection.bbox").alias("bbox"),
            coalesce(col("detection.entry_time"), lit("1970-01-01 00:00:00")).alias("entry_time"),
            coalesce(col("detection.exit_time"), lit("1970-01-01 00:00:00")).alias("exit_time")
        )

        logging.info("Cleaned and filled fields with default values where necessary.")
        return cleaned_df
    except Exception as e:
        logging.error(f"Error during field cleanup: {e}")
        raise


# preprocessing/timestamp_format.py
from pyspark.sql.functions import to_timestamp, col, coalesce, lit
import logging

def normalize_timestamps(df):
    try:
        default_time = lit("1970-01-01 00:00:00")
        normalized_df = df.withColumn("entry_time", coalesce(to_timestamp(col("entry_time"), "yyyy-MM-dd HH:mm:ss"), default_time)) \
                          .withColumn("exit_time", coalesce(to_timestamp(col("exit_time"), "yyyy-MM-dd HH:mm:ss"), default_time))

        logging.info("Timestamps normalized and nulls filled with default time.")
        return normalized_df
    except Exception as e:
        logging.error(f"Error parsing timestamps: {e}")
        raise
