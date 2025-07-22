from pyspark.sql.functions import col, explode, struct, collect_list, array, lit, coalesce, from_json, to_json, map_from_entries, count
from pyspark.sql.types import *
from .common import *
import logging

def process_parking_json_data(df):
    if "frame_detections" not in df.columns:
        logging.warning("Column 'frame_detections' not found in the input DataFrame. Skipping processing.")
        return df, -1

    try:
        default_config = {
            "default_values": {
                "occupied": False,
                "bbox": [0.0, 0.0, 0.0, 0.0],
            }
        }

        # Explode the frame_detections array
        exploded = df.withColumn("frame", explode(col("frame_detections")))
        top_level_fields = [c for c in df.columns if c != "frame_detections"]

        frame_fields = exploded.select(
            *[col(f) for f in top_level_fields],
            col("frame.frame_number"),
            col("frame.timestamp_sec"),
            col("frame.slots").alias("slots"),
            col("frame.free_slots").alias("original_free_slots")
        )

        # Parse slots (MapType with Struct values)
        frame_fields = frame_fields.withColumn("slots_json", to_json(col("slots")))
        frame_fields = frame_fields.withColumn("slots_map", from_json(
            col("slots_json"),
            MapType(StringType(), StructType([
                StructField("occupied", BooleanType()),
                StructField("bbox", ArrayType(DoubleType()))  # Use DoubleType
            ]))
        )).drop("slots", "slots_json").withColumnRenamed("slots_map", "slots")

        # Explode slots
        slot_fields = frame_fields.select(
            *[col(f) for f in top_level_fields],
            col("frame_number"),
            col("timestamp_sec"),
            explode(col("slots")).alias("slot_number", "slot_details")
        )

        processed = slot_fields.select(
            *[col(f) for f in top_level_fields],
            col("frame_number"),
            col("timestamp_sec"),
            col("slot_number"),
            col("slot_details.occupied").alias("occupied"),
            col("slot_details.bbox").alias("bbox")
        )

        processed = clean_string_columns(processed)
        processed = handle_null_values(processed, default_config["default_values"])
        processed = processed.withColumn("bbox", col("bbox").cast(ArrayType(DoubleType())))

        processed = (
            processed
            .withColumn("bbox_x1", col("bbox")[0])
            .withColumn("bbox_y1", col("bbox")[1])
            .withColumn("bbox_x2", col("bbox")[2])
            .withColumn("bbox_y2", col("bbox")[3])
        )

        slot_struct = processed.select(
            *[col(f) for f in top_level_fields],
            "frame_number",
            "timestamp_sec",
            "slot_number",
            struct(
                "occupied", "bbox", "bbox_x1", "bbox_y1", "bbox_x2", "bbox_y2"
            ).alias("slot_details")
        )

        grouped_by_frame = slot_struct.groupBy(
            *[col(f) for f in top_level_fields],
            "frame_number", "timestamp_sec"
        ).agg(
            collect_list(struct("slot_number", "slot_details")).alias("slots")
        )

        # Calculate free_slots (count of unoccupied slots)
        free_slots_calc = processed.filter(~col("occupied")).groupBy(
            *[col(f) for f in top_level_fields],
            "frame_number", "timestamp_sec"
        ).agg(count("*").alias("free_slots"))

        final_frames = grouped_by_frame.join(
            free_slots_calc,
            on=[*top_level_fields, "frame_number", "timestamp_sec"],
            how="left"
        ).withColumn("free_slots", coalesce(col("free_slots"), lit(0)))

        final_frames = final_frames.withColumn("slots", map_from_entries(col("slots")))

        result_df = final_frames.orderBy("frame_number").groupBy(
            *[col(f) for f in top_level_fields]
        ).agg(
            collect_list(struct(
                "frame_number", "timestamp_sec", "slots", "free_slots"
            )).alias("frame_detections")
        ).select(
            *[col(f) for f in top_level_fields],
            col("frame_detections")
        )

        return result_df, 1

    except Exception as e:
        logging.error(f"Error processing parking lot detection data: {str(e)}")
        return df, -1
