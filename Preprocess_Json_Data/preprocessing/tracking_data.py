from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, explode, to_timestamp, struct, array, regexp_replace
from pyspark.sql.types import StringType
from typing import Dict, Optional
import logging
from .common import *

def process_tracking_data(df: DataFrame, config: Optional[Dict] = None) -> DataFrame:
    """
    Process tracking data with struct detections
    """
    default_config = {
        "required_fields": ["detections"],
        "timestamp_format": "yyyy-MM-dd HH:mm:ss",
        "default_values": {
            "age": -1,
            "mask_confidence": 0.0,
            "confidence": 0.5,
            "gender": "Unknown"
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
        
        # Convert to array column
        df = df.withColumn("detections_array", array(*detection_structs))
        
        # Explode the array
        df = df.withColumn("detection", explode("detections_array"))
        
        # Select and flatten fields - explicitly rename tracker_id to avoid ambiguity
        df = df.select(
            col("video_metadata"),
            col("processing_time"),
            col("summary"),
            col("detection.tracker_id_str").alias("tracker_id_str"),
            col("detection.details.*")
        ).drop("tracker_id")  # Drop the duplicate tracker_id from details if it exists
        
        # Ensure all expected fields exist
        detection_fields = [
            "gender", "age", "carrying", "confidence",
            "entry_time", "exit_time", "entry_frame", "exit_frame"
        ]
        
        for field in detection_fields:
            if field not in df.columns:
                df = df.withColumn(field, lit(None))
        
        # Apply common preprocessing
        df = handle_null_values(df, config["default_values"])
        
        # Clean string columns
        string_cols = [f.name for f in df.schema.fields 
                      if isinstance(f.dataType, StringType) 
                      and f.name != "tracker_id_str"]
        for col_name in string_cols:
            df = df.withColumn(col_name, col(col_name).cast(StringType()))
        
        # Clean timestamp strings by removing timezone info
        timestamp_cols = [col for col in ["entry_time", "exit_time", "processing_time"] 
                        if col in df.columns]
        for col_name in timestamp_cols:
            df = df.withColumn(col_name, regexp_replace(col(col_name), " UTC", ""))
        
        # Convert timestamps
        if timestamp_cols:
            df = convert_timestamps(df, timestamp_cols, config["timestamp_format"])
        
        return df
        
    except Exception as e:
        logging.error(f"Error processing tracking data: {e}")
        raise