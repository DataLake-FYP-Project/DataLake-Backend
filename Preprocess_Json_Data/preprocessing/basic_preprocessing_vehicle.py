
from pyspark.sql.functions import col, explode, struct, collect_list, array, lit, coalesce, size
from pyspark.sql.types import *
from .common import *


def process_vehicle_json_data(df):
    # First validate required columns exist
    required_columns = ["frame_number", "detections"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"Required columns {missing_columns} not found in input DataFrame. Skipping processing.")
        return df, -1

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
        },
        "timestamp_fields": ["entry_time", "exit_time", "red_light_violation_time", 
                           "line_crossing_violation_time", "vehicle_entry_time", "vehicle_exit_time"],
        "preserve_null_fields": ["entry_time", "exit_time", "red_light_violation_time", 
                              "line_crossing_violation_time", "vehicle_entry_time", "vehicle_exit_time"]
    }
    config = default_config | {}

    try:
        # Check if there are any detections at all
        total_detections = df.select(size(col("detections")).alias("count")).agg({"count": "sum"}).collect()[0][0]
        if total_detections == 0:
            print("No detections found in any frame. Returning original DataFrame.")
            return df,-1

        # Select only available frame-level columns
        frame_columns = [col for col in ["frame_number", "congestion_level", "traffic_light"] 
                        if col in df.columns]
        
        # Step 1: Explode detections and apply preprocessing
        processed = df.withColumn("detection", explode("detections")) \
            .select(
                *[col(c) for c in frame_columns],
                col("detection.*")
            )

        # Handle renamed fields - only if they exist
        if "vehicle_speed" in processed.columns:
            processed = processed.withColumnRenamed("vehicle_speed", "speed")
        if "vehicle_direction" in processed.columns:
            processed = processed.withColumnRenamed("vehicle_direction", "direction")
        if "vehicle_lane" in processed.columns:
            processed = processed.withColumnRenamed("vehicle_lane", "lane")
        if "vehicle_type" in processed.columns:
            processed = processed.withColumnRenamed("vehicle_type", "class_name")
        if "vehicle_entry_time" in processed.columns:
            processed = processed.withColumnRenamed("vehicle_entry_time", "entry_time")
        if "vehicle_exit_time" in processed.columns:
            processed = processed.withColumnRenamed("vehicle_exit_time", "exit_time")

        # Apply preprocessing pipeline only to existing columns
        processed = (
            processed
            # Type conversion and null handling
            .transform(lambda df: handle_null_values(df, config["default_values"]))
            # String processing
            .transform(lambda df: clean_string_columns(df))
            # Temporal fields - only convert existing ones
            .transform(lambda df: convert_timestamps(
                df, 
                [f for f in config["timestamp_fields"] if f in df.columns]
            ))
        )

        # BBOX processing if available
        if "bbox" in processed.columns:
            processed = (
                processed.withColumn("bbox", col("bbox").cast(ArrayType(DoubleType())))
                .withColumn("bbox_x1", col("bbox")[0])
                .withColumn("bbox_y1", col("bbox")[1])
                .withColumn("bbox_x2", col("bbox")[2])
                .withColumn("bbox_y2", col("bbox")[3])
            )

        # Step 2: Reconstruct detection objects with only available fields
        detection_fields = []
        possible_fields = [
            ("tracker_id", "tracker_id"),
            ("class_id", "class_id"),
            ("class_name", "class_name"),
            ("confidence", "confidence"),
            ("bbox", "bbox"),
            ("bbox_x1", "bbox_x1"),
            ("bbox_y1", "bbox_y1"),
            ("bbox_x2", "bbox_x2"),
            ("bbox_y2", "bbox_y2"),
            ("direction", "direction"),
            ("lane", "lane"),
            ("vehicle_color", "vehicle_color"),
            ("stopped", "stopped"),
            ("speed", "speed"),
            ("entry_time", "entry_time"),
            ("exit_time", "exit_time"),
            ("red_light_violation", "red_light_violation"),
            ("red_light_violation_time", "red_light_violation_time"),
            ("line_crossing", "line_crossing"),
            ("line_crossing_violation_time", "line_crossing_violation_time")
        ]
        
        for field_name, col_name in possible_fields:
            if col_name in processed.columns:
                detection_fields.append(col(col_name).alias(field_name))

        reconstructed_detections = processed.select(
            *[col(c) for c in frame_columns],
            struct(*detection_fields).alias("detection")
        )

        # Step 3: Regroup detections by frame
        result = (
            reconstructed_detections
            .groupBy(*frame_columns)
            .agg(collect_list("detection").alias("detections"))
            .orderBy("frame_number")
            .select(
                struct(
                    *[col(c) for c in frame_columns],
                    col("detections")
                ).alias("frame_data")
            )
        )

        return result, 1

    except Exception as e:
        print(f"Error processing vehicle detection data: {str(e)}")
        return df, -1