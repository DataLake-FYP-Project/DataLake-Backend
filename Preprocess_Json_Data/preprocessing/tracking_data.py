from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, explode, to_timestamp, struct, array, regexp_replace, collect_list
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType
from typing import Dict, Optional
import logging
from preprocessing.common import *

TRACKING_DATA_SCHEMA = StructType([
    StructField("tracker_id", StringType(), False),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("carrying", StringType(), True),
    StructField("confidence", DoubleType(), True),
    StructField("entry_time", StringType(), True),
    StructField("exit_time", StringType(), True),
    StructField("entry_frame", IntegerType(), True),
    StructField("exit_frame", IntegerType(), True),
    StructField("processing_time", StringType(), True),
    StructField("video_metadata", StringType(), True),
    StructField("summary", StringType(), True)
])

def process_tracking_data(df: DataFrame, config: Optional[Dict] = None) -> DataFrame:
    """Process tracking data with enhanced schema validation"""
    default_config = {
        "required_fields": ["detections"],
        "timestamp_format": "yyyy-MM-dd HH:mm:ss",
        "default_values": {
            "age": -1,
            "confidence": 0.5,
            "gender": "Unknown",
            "carrying": "Unknown"
        }
    }
    
    config = {**default_config, **(config or {})}
    
    try:
        # Verify required fields
        for field in config["required_fields"]:
            if field not in df.columns:
                raise ValueError(f"Missing required field: {field}")
        
        # Create array of structs for each detection
        detection_structs = [
            struct(
                lit(key).alias("tracker_id_str"),
                col(f"detections.{key}").alias("details")
            )
            for key in ["10", "15", "2", "4", "5", "6"]  # All possible tracker IDs
        ]
        
        # Process detections
        processed_df = (
            df
            .withColumn("detections_array", array(*detection_structs))
            .withColumn("detection", explode("detections_array"))
            .select(
                col("video_metadata"),
                col("processing_time"),
                col("summary"),
                col("detection.tracker_id_str").alias("tracker_id_str"),
                col("detection.details.*")
            )
            .drop("tracker_id")  # Remove duplicate if exists
        )
        
        # Ensure all expected fields exist
        detection_fields = [
            "gender", "age", "carrying", "confidence",
            "entry_time", "exit_time", "entry_frame", "exit_frame"
        ]
        
        for field in detection_fields:
            if field not in processed_df.columns:
                processed_df = processed_df.withColumn(field, lit(None))
        
        # Apply common preprocessing
        processed_df = handle_null_values(processed_df, config["default_values"])
        
        # Clean string columns
        string_cols = [f.name for f in processed_df.schema.fields 
                      if isinstance(f.dataType, StringType) 
                      and f.name != "tracker_id_str"]
        for col_name in string_cols:
            processed_df = processed_df.withColumn(col_name, col(col_name).cast(StringType()))
        
        # Clean timestamp strings by removing timezone info
        timestamp_cols = [col for col in ["entry_time", "exit_time", "processing_time"] 
                        if col in processed_df.columns]
        for col_name in timestamp_cols:
            processed_df = processed_df.withColumn(
                col_name, 
                regexp_replace(col(col_name), " UTC", "")
            )
        
        # Convert timestamps
        if timestamp_cols:
            processed_df = convert_timestamps(processed_df, timestamp_cols, config["timestamp_format"])
        
        # Validate output schema
        for field in TRACKING_DATA_SCHEMA.fields:
            if field.name not in processed_df.columns:
                processed_df = processed_df.withColumn(field.name, lit(None).cast(field.dataType))
        
        return processed_df.select([field.name for field in TRACKING_DATA_SCHEMA.fields])
        
    except Exception as e:
        logging.error(f"Error processing tracking data: {e}")
        raise