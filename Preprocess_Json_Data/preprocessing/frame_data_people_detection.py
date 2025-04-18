from pyspark.sql.functions import col, explode, struct, collect_list, to_json, from_json, array, lit
from pyspark.sql.types import *
from pyspark.sql.functions import coalesce, array
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


def process_people_json_data(df, options):
    if "frame_detections" not in df.columns:
        print("Column 'frame_detections' not found in the input DataFrame. Skipping processing.")
        return df

    # Detection schema
    detection_schema = StructType([
        StructField("tracker_id", LongType()),
        StructField("class_id", LongType()),
        StructField("class_name", StringType()),
        StructField("confidence", DoubleType()),
        StructField("bbox", ArrayType(DoubleType())),
        StructField("in_area1", BooleanType()),
        StructField("in_area2", BooleanType()),
        StructField("in_restricted_area", BooleanType()),
        StructField("gender", StringType()),
        StructField("age", StringType()),
        StructField("carrying", StringType()),
        StructField("mask_status", StringType()),
        StructField("mask_confidence", DoubleType()),
        StructField("entry_time", StringType()),
        StructField("exit_time", StringType()),
        StructField("first_seen_frame", LongType()),
        StructField("last_seen_frame", LongType()),
        StructField("entered_restricted", BooleanType())
    ])

    frame_schema = StructType([
        StructField("frame_number", LongType()),
        StructField("timestamp", StringType()),
        StructField("detections", ArrayType(detection_schema))
    ])

    # Step 1: Explode each frame
    exploded = df.withColumn("frame", explode(col("frame_detections")))

    # Step 2: Extract frame fields
    exploded = exploded.select(
        col("video_metadata"),
        col("summary"),
        col("processing_time"),
        col("frame.frame_number").alias("frame_number"),
        col("frame.timestamp").alias("timestamp"),
        col("frame.detections")
    )

    # Step 3: Explode detections within each frame (may be empty)
    exploded_detections = exploded.withColumn("detection", explode("detections")).select(
        "video_metadata", "summary", "processing_time", "frame_number", "timestamp",
        col("detection.tracker_id"),
        col("detection.class_id"),
        col("detection.class_name"),
        col("detection.confidence"),
        col("detection.bbox").getItem(0).alias("bbox_x1"),
        col("detection.bbox").getItem(1).alias("bbox_y1"),
        col("detection.bbox").getItem(2).alias("bbox_x2"),
        col("detection.bbox").getItem(3).alias("bbox_y2"),
        col("detection.in_area1"),
        col("detection.in_area2"),
        col("detection.in_restricted_area"),
        col("detection.gender"),
        col("detection.age"),
        col("detection.carrying"),
        col("detection.mask_status"),
        col("detection.mask_confidence"),
        col("detection.entry_time"),
        col("detection.exit_time"),
        col("detection.first_seen_frame"),
        col("detection.last_seen_frame"),
        col("detection.entered_restricted")
    ).filter(col("confidence") > options.get("confidence_threshold", 0.7))

    # Step 4: Re-group detections back into frames
    regrouped_detections = exploded_detections.groupBy(
        "video_metadata", "summary", "processing_time", "frame_number", "timestamp"
    ).agg(
        collect_list(
            struct(
                "tracker_id",
                "class_id",
                "class_name",
                "confidence",
                struct("bbox_x1", "bbox_y1", "bbox_x2", "bbox_y2").alias("bbox"),
                "in_area1",
                "in_area2",
                "in_restricted_area",
                "gender",
                "age",
                "carrying",
                "mask_status",
                "mask_confidence",
                "entry_time",
                "exit_time",
                "first_seen_frame",
                "last_seen_frame",
                "entered_restricted"
            )
        ).alias("detections")
    )

    # Step 5: Join with original exploded frames to retain frames with empty detections
    all_frames = exploded.select(
        "video_metadata", "summary", "processing_time", "frame_number", "timestamp"
    ).dropDuplicates()

    final_frames = all_frames.join(
        regrouped_detections,
        on=["video_metadata", "summary", "processing_time", "frame_number", "timestamp"],
        how="left"
    ).withColumn(
        "detections", coalesce(col("detections"), array())
    )



    # Ensure frames are ordered before grouping
    ordered_frames = final_frames.orderBy("frame_number").select(
        "video_metadata",
        "summary",
        "processing_time",
        struct(
            col("frame_number"),
            col("timestamp"),
            col("detections")
        ).alias("frame_struct")
    )

    # Re-group all frames and construct top-level object manually
    result_df = ordered_frames.groupBy("video_metadata", "summary", "processing_time").agg(
        collect_list("frame_struct").alias("frame_detections")
    ).select(
        col("video_metadata"),
        col("summary"),
        col("processing_time"),
        col("frame_detections")
    )


    return result_df
