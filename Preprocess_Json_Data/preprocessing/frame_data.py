from pyspark.sql.functions import col, explode, struct, collect_list, array, lit, coalesce
from pyspark.sql.types import *
from preprocessing.common import *

def process_frame_data(df):
    if "detections" not in df.columns:
        print("Column 'detections' not found in the input DataFrame. Skipping processing.")
        return df

    # Configuration with default values for vehicle detection
    default_config = {
        "confidence_threshold": 0.7,
        "default_values": {
            "confidence": 0.5,
            "tracker_id": -1,
            "class_id": -1,
            "direction": "unknown",
            "lane": "unknown",
            "vehicle_color": "unknown",
            "stopped": False,
            "speed": 0.0,
            "entry_time": None,
            "exit_time": None
        },
        "timestamp_fields": ["entry_time", "exit_time"],  
        "preserve_null_fields": ["entry_time", "exit_time"]
    }
    config = default_config | {}

    # Step 1: Explode detections and apply preprocessing
    processed = df.withColumn("detection", explode("detections")) \
                .select(
                    col("frame_number"),
                    col("congestion_level"),
                    col("detection.*")
                )

    # Apply preprocessing pipeline
    processed = (
        processed
        # Type conversion and null handling
        .transform(lambda df: handle_null_values(df, config["default_values"]))
        
        # String processing
        .transform(lambda df: clean_string_columns(df))
        
        # Temporal fields (only entry_time and exit_time)
        .transform(lambda df: convert_timestamps(df, config["timestamp_fields"]))
        
        # BBOX processing
        .withColumn("bbox", col("bbox").cast(ArrayType(DoubleType())))
        .withColumn("bbox_x1", col("bbox")[0])
        .withColumn("bbox_y1", col("bbox")[1])
        .withColumn("bbox_x2", col("bbox")[2])
        .withColumn("bbox_y2", col("bbox")[3])
    )

    # Step 2: Reconstruct detection objects (without timestamp field)
    reconstructed_detections = processed.select(
        "frame_number", "congestion_level",
        struct(
            col("tracker_id"),
            col("class_id"),
            col("class_name"),
            col("confidence"),
            col("bbox"),
            col("bbox_x1"),
            col("bbox_y1"),
            col("bbox_x2"),
            col("bbox_y2"),
            col("direction"),
            col("lane"),
            col("vehicle_color"),
            col("stopped"),
            col("speed"),
            col("entry_time"),
            col("exit_time")
        ).alias("detection")
    )

    # Step 3: Regroup detections by frame
    result = (
        reconstructed_detections
        .groupBy("frame_number", "congestion_level")
        .agg(collect_list("detection").alias("detections"))
        .orderBy("frame_number")
        .select(
            struct(
                col("frame_number"),
                col("congestion_level"),
                col("detections")
            ).alias("frame_data")
        )
    )

    return result