from pyspark.sql.functions import col, explode, struct, collect_list, size
from pyspark.sql.types import *
from process_scripts.common import clean_string_columns, convert_timestamps, handle_null_values

def process_school_json_data(df):
    required_columns = ["frame_number", "timestamp", "detections"]
    missing_columns = [c for c in required_columns if c not in df.columns]
    
    if missing_columns:
        print(f"Missing required columns: {missing_columns}. Skipping processing.")
        return df, -1

    default_config = {
        "confidence_threshold": 0.7,
        "default_values": {
            "event_id": "unknown",
            "event_type": "unknown",
            "timestamp": None,
            "location": "unknown",
            "confidence": 0.0,
            "involved_person_id": "unknown",
            "duration_seconds": 0,
            "notes": "",
            "alert_level": "low",
            "response_required": False,
            "multiple_persons_involved": False,
            "person_roles": []
        },
        "timestamp_fields": ["timestamp"],
        "preserve_null_fields": ["timestamp"]
    }

    config = default_config | {}

    try:
        total_detections = df.select(size(col("detections")).alias("count")).agg({"count": "sum"}).collect()[0][0]
        if total_detections == 0:
            print("No detections found. Returning original DataFrame.")
            return df, -1
        
        frame_columns = [col for col in ["frame_number", "timestamp", "detections"] 
                        if col in df.columns]
        
        processed = df.withColumn("detection", explode("detections")) \
            .select(
                *[col(c) for c in frame_columns],
                col("detection.*")
            )
        
        processed = (
            processed
            .transform(lambda df: handle_null_values(df, config["default_values"]))
            .transform(lambda df: clean_string_columns(df))
            .transform(lambda df: convert_timestamps(
                df, 
                [f for f in config["timestamp_fields"] if f in df.columns]
            ))
        )

        if "bbox" in processed.columns:
            processed = (
                processed.withColumn("bbox", col("bbox").cast(ArrayType(DoubleType())))
                .withColumn("bbox_x1", col("bbox")[0])
                .withColumn("bbox_y1", col("bbox")[1])
                .withColumn("bbox_x2", col("bbox")[2])
                .withColumn("bbox_y2", col("bbox")[3])
            )

        detection_fields = []
        possible_fields = [
            ("event_id", "event_id"),
            ("event_type", "event_type"),
            ("timestamp", "timestamp"),
            ("location", "location"),
            ("confidence", "confidence"),
            ("involved_person_id", "involved_person_id"),
            ("duration_seconds", "duration_seconds"),
            ("notes", "notes"),
            ("alert_level", "alert_level"),
            ("response_required", "response_required"),
            ("multiple_persons_involved", "multiple_persons_involved"),
            ("person_roles", "person_roles")
        ]

        for field_name, col_name in possible_fields:
            if col_name in processed.columns:
                detection_fields.append(col(col_name).alias(field_name))

        reconstructed_detections = processed.select(
            *[col(c) for c in frame_columns],
            struct(*detection_fields).alias("detection")
        )

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
        print(f"Error processing school detection data: {str(e)}")
        return df, -1