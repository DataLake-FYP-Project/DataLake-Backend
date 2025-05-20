from pyspark.sql.functions import col, explode, struct, collect_list, array, lit, coalesce,size,explode_outer
from pyspark.sql.types import *
from .common import *


def process_people_json_data(df):
    if "frame_detections" not in df.columns:
        print("Column 'frame_detections' not found in the input DataFrame. Skipping processing.")
        return df,-1
    else:
        # Configuration with default values
        default_config = {
            "default_values": {
                "confidence": 0.0,
                "tracker_id": -1,
                "class_id": -1,
                "gender": "unknown",
                "age": -1,
                "carrying": "none",
                "mask_status": "unknown",
                "mask_confidence": 0.0,
                "in_restricted_area": False,
                "entry_time": "2101-01-29 17:53:46",
                "exit_time": "2101-01-29 17:53:46"
            },
            "timestamp_fields": ["timestamp", "entry_time", "exit_time"],
            "preserve_null_fields": ["entry_time", "exit_time"]
        }
        config = default_config | {}
        try:
            # Step 1: Explode frame_detections array
            exploded = df.withColumn("frame", explode(col("frame_detections")))
                
            # Extract detections to check for empty
            frame_fields_check = exploded.select(
                col("frame.detections").alias("detections")
            )

            non_empty_count = frame_fields_check.filter(size(col("detections")) > 0).count()
            if non_empty_count == 0:
                logging.info("No detections to process. Returning metadata only.")
                return df.select(
                    *[col(f) for f in ["video_metadata", "summary", "processing_time"]
                      if f in df.columns]
                ).withColumn("processing_status", lit("no_detections")),-1

            # Step 2: Process frame-level fields - only available columns
            top_level_fields = [c for c in ["video_metadata", "summary"] 
                                if c in exploded.columns]
            
            frame_fields = exploded.select(
                *[col(f) for f in top_level_fields],
                col("processing_time"),
                col("frame.frame_number"),
                col("frame.timestamp"),
                col("frame.detections")
            )

            # Step 3: Safely explode detections
            processed_detections = frame_fields.withColumn("detection", explode_outer("detections"))
            
            # Step 3a: Verify detection is a struct before expanding
            if isinstance(processed_detections.schema["detection"].dataType, StructType):
                detection_columns = [
                    *[col(f) for f in top_level_fields],
                    col("processing_time"),
                    col("frame_number"),
                    col("timestamp"),
                    col("detection.*")
                ]
                processed_detections = processed_detections.select(*detection_columns)
            else:
                print("Warning: detections are not in struct format")
                processed_detections = processed_detections.select(
                "video_metadata", "summary", "processing_time",
                "frame_number", "timestamp", "detection"
            )

            # Apply common preprocessing
            processed_detections = clean_string_columns(processed_detections)
            processed_detections = handle_null_values(processed_detections, config["default_values"])
            
            # Convert existing timestamp fields only
            existing_timestamp_fields = [
                f for f in config["timestamp_fields"] 
                if f in processed_detections.columns
            ]
            processed_detections = convert_timestamps(processed_detections, existing_timestamp_fields)

            # BBOX processing if available
            if "bbox" in processed_detections.columns:
                processed_detections = (
                    processed_detections.withColumn("bbox", col("bbox").cast(ArrayType(DoubleType())))
                    .withColumn("bbox_x1", col("bbox")[0])
                    .withColumn("bbox_y1", col("bbox")[1])
                    .withColumn("bbox_x2", col("bbox")[2])
                    .withColumn("bbox_y2", col("bbox")[3])
                )

            # Step 4: Reconstruct detections with available fields
            detection_fields = []
            field_mapping = [
                ("tracker_id", "tracker_id"),
                ("class_id", "class_id"),
                ("class_name", "class_name"),
                ("confidence", "confidence"),
                ("bbox", "bbox"),
                ("bbox_x1", "bbox_x1"),
                ("bbox_y1", "bbox_y1"),
                ("bbox_x2", "bbox_x2"),
                ("bbox_y2", "bbox_y2"),
                ("in_restricted_area", "in_restricted_area"),
                ("gender", "gender"),
                ("age", "age"),
                ("carrying", "carrying"),
                ("entry_time", "entry_time"),
                ("exit_time", "exit_time"),
                ("first_seen_frame", "first_seen_frame"),
                ("last_seen_frame", "last_seen_frame")
            ]
            
            for field_name, col_name in field_mapping:
                if col_name in processed_detections.columns:
                    detection_fields.append(col(col_name).alias(field_name))

            if not detection_fields:
                logging.info("No valid detection fields found")
                return df,-1

            reconstructed_detections = processed_detections.select(
                *[col(f) for f in top_level_fields],
                col("processing_time"),
                col("frame_number"),
                col("timestamp"),
                struct(*detection_fields).alias("detection")
            )

            # Step 5: Regroup detections by frame
            grouping_fields = [
                *[f for f in top_level_fields],
                "processing_time",
                "frame_number",
                "timestamp"
            ]
            grouped_by_frame = reconstructed_detections.groupBy(*grouping_fields).agg(
                collect_list("detection").alias("detections")
            )

            # Step 6: Join with all frames to preserve empty detections
            all_frames = frame_fields.select(
                *[col(f) for f in top_level_fields],
                col("processing_time"),
                col("frame_number"),
                col("timestamp")
            ).distinct()

            final_frames = all_frames.join(
                grouped_by_frame,
                on=[f for f in grouping_fields if f in all_frames.columns and f in grouped_by_frame.columns],
                how="left"
            ).withColumn(
                "detections", coalesce(col("detections"), array())
            )

            # Step 7: Reconstruct the original JSON structure
            result_df = final_frames.orderBy("frame_number").groupBy(
                *[col(f) for f in top_level_fields],col("processing_time")
            ).agg(
                collect_list(
                    struct(
                        *[col(c) for c in ["frame_number", "timestamp", "detections"] 
                            if c in final_frames.columns]
                    )
                ).alias("frame_detections")
            ).select(
                *[col(f) for f in top_level_fields],
                col("processing_time"),
                col("frame_detections")
            )

            return result_df,1

        except Exception as e:
            print(f"Error processing people detection data: {str(e)}")
            return df,-1


