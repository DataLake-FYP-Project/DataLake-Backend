from pyspark.sql.functions import col, explode, struct, collect_list, array, lit, coalesce
from pyspark.sql.types import *
from .common import *


def process_people_json_data(df):
    if "frame_detections" not in df.columns:
        print("Column 'frame_detections' not found in the input DataFrame. Skipping processing.")
        return df

    # Configuration with default values
    default_config = {
        "default_values": {
            "confidence": 0.0,
            "tracker_id": -1,
            "class_id": -1,
            "gender": "unknown",
            "age": -1,
            "carrying": "none",
            # "mask_status": "unknown",
            # "mask_confidence": 0.0,
            "in_restricted_area": False,
            "entry_time": "2101-01-29 17:53:46",
            "exit_time": "2101-01-29 17:53:46"
        },
        "timestamp_fields": ["timestamp", "entry_time", "exit_time"],
        "preserve_null_fields": ["entry_time", "exit_time"]
    }
    config = default_config | {}

    # Step 1: Explode frame_detections array
    exploded = df.withColumn("frame", explode(col("frame_detections")))

    # Step 2: Process frame-level fields
    frame_fields = exploded.select(
        col("video_metadata"),
        col("summary"),
        col("processing_time"),
        col("frame.frame_number"),
        col("frame.timestamp"),
        col("frame.detections")
    )

    # Step 3: Explode detections and apply preprocessing
    processed_detections = frame_fields.withColumn("detection", explode("detections")).select(
        "video_metadata", "summary", "processing_time", "frame_number", "timestamp",
        col("detection.*")
    )

    # Apply common preprocessing
    processed_detections = clean_string_columns(processed_detections)
    processed_detections = handle_null_values(processed_detections, config["default_values"])
    processed_detections = convert_timestamps(processed_detections, config["timestamp_fields"])

    # Extract bbox coordinates while keeping original bbox array
    processed_detections = processed_detections.withColumn(
        "bbox", col("bbox").cast(ArrayType(DoubleType()))
    ).withColumn("bbox_x1", col("bbox")[0]) \
        .withColumn("bbox_y1", col("bbox")[1]) \
        .withColumn("bbox_x2", col("bbox")[2]) \
        .withColumn("bbox_y2", col("bbox")[3])

    # Step 4: Reconstruct detections with original structure plus new fields
    reconstructed_detections = processed_detections.select(
        "video_metadata", "summary", "processing_time", "frame_number", "timestamp",
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
            col("in_restricted_area"),
            col("gender"),
            col("age"),
            col("carrying"),
            # col("mask_status"),
            # col("mask_confidence"),
            col("entry_time"),
            col("exit_time"),
            col("first_seen_frame"),
            col("last_seen_frame")
        ).alias("detection")
    )

    # Step 5: Regroup detections by frame
    grouped_by_frame = reconstructed_detections.groupBy(
        "video_metadata", "summary", "processing_time", "frame_number", "timestamp"
    ).agg(
        collect_list("detection").alias("detections")
    )

    # Step 6: Join with all frames to preserve empty detections
    all_frames = frame_fields.select(
        "video_metadata", "summary", "processing_time", "frame_number", "timestamp"
    ).distinct()

    final_frames = all_frames.join(
        grouped_by_frame,
        on=["video_metadata", "summary", "processing_time", "frame_number", "timestamp"],
        how="left"
    ).withColumn(
        "detections", coalesce(col("detections"), array())
    )

    # Step 7: Reconstruct the original JSON structure
    result_df = final_frames.orderBy("frame_number").groupBy(
        "video_metadata", "summary", "processing_time"
    ).agg(
        collect_list(
            struct(
                col("frame_number"),
                col("timestamp"),
                col("detections")
            )
        ).alias("frame_detections")
    ).select(
        "video_metadata", "summary", "processing_time", "frame_detections"
    )

    return result_df
