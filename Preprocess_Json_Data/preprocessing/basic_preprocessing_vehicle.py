from pyspark.sql.functions import col, explode, struct, collect_list, array, lit, coalesce
from pyspark.sql.types import *
from .common import *


def process_vehicle_json_data(df):
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
            "exit_time": None,
            "red_light_violation": False,
            "red_light_violation_time": None,
            "line_crossing": False,
            "line_crossing_violation_time": None,
            "vehicle_type": "unknown"
            # "latitude": 0.0,
            # "longitude": 0.0
        },
        "timestamp_fields": ["entry_time", "exit_time", "red_light_violation_time", "line_crossing_violation_time",
                             "vehicle_entry_time", "vehicle_exit_time"],
        "preserve_null_fields": ["entry_time", "exit_time", "red_light_violation_time", "line_crossing_violation_time",
                                 "vehicle_entry_time", "vehicle_exit_time"]
    }
    config = default_config | {}

    # Step 1: Explode detections and apply preprocessing
    processed = df.withColumn("detection", explode("detections")) \
        .select(
        col("frame_number"),
        col("congestion_level"),
        col("traffic_light"),
        col("detection.*")
    )

    # Process geolocation
    # processed = processed.withColumn("latitude", col("geolocation.latitude")) \
    # .withColumn("longitude", col("geolocation.longitude")) \
    # .drop("geolocation")

    # Handle renamed fields
    processed = processed.withColumnRenamed("vehicle_speed", "speed") \
        .withColumnRenamed("vehicle_direction", "direction") \
        .withColumnRenamed("vehicle_lane", "lane") \
        .withColumnRenamed("vehicle_color", "vehicle_color") \
        .withColumnRenamed("vehicle_type", "class_name") \
        .withColumnRenamed("vehicle_entry_time", "entry_time") \
        .withColumnRenamed("vehicle_exit_time", "exit_time")

    # Apply preprocessing pipeline
    processed = (
        processed
        # Type conversion and null handling
        .transform(lambda df: handle_null_values(df, config["default_values"]))

        # String processing
        .transform(lambda df: clean_string_columns(df))

        # Temporal fields
        .transform(lambda df: convert_timestamps(df, config["timestamp_fields"]))

        # BBOX processing
        .withColumn("bbox", col("bbox").cast(ArrayType(DoubleType())))
        .withColumn("bbox_x1", col("bbox")[0])
        .withColumn("bbox_y1", col("bbox")[1])
        .withColumn("bbox_x2", col("bbox")[2])
        .withColumn("bbox_y2", col("bbox")[3])
    )

    # Step 2: Reconstruct detection objects
    reconstructed_detections = processed.select(
        "frame_number", "congestion_level", "traffic_light",
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
            col("exit_time"),
            col("red_light_violation"),
            col("red_light_violation_time"),
            col("line_crossing"),
            col("line_crossing_violation_time")
            # struct(
            #  col("latitude"),
            #   col("longitude")
            # ).alias("geolocation")
        ).alias("detection")
    )

    # Step 3: Regroup detections by frame
    result = (
        reconstructed_detections
        .groupBy("frame_number", "congestion_level", "traffic_light")
        .agg(collect_list("detection").alias("detections"))
        .orderBy("frame_number")
        .select(
            struct(
                col("frame_number"),
                col("congestion_level"),
                col("traffic_light"),
                col("detections")
            ).alias("frame_data")
        )
    )

    return result
