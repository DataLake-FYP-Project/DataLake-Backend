from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, struct, lit
from typing import Dict, Optional
import logging
from preprocessing.common import *

def process_frame_data(df: DataFrame, config: Optional[Dict] = None) -> DataFrame:
    """
    Process frame-based detection data with schema:
    {
        "frame_number": int,
        "detections": [
            {
                "tracker_id": int,
                "class_id": int,
                "confidence": float,
                "bbox": [float, float, float, float],
                ...
            }
        ]
    }
    """
    # Default configuration
    default_config = {
        "required_fields": ["frame_number", "detections"],
        "confidence_threshold": 0.7,
        "default_values": {
            "confidence": 0.5,
            "tracker_id": -1,
            "class_id": -1
        }
    }
    
    # Merge with user config
    config = {**default_config, **(config or {})}
    
    try:
        # Validate input schema
        required_fields = config["required_fields"]
        for field in required_fields:
            if field not in df.columns:
                raise ValueError(f"Missing required field: {field}")
        
        # Explode detections array
        df = df.withColumn("detection", explode("detections"))
        
        # Select and flatten detection fields
        detection_fields = [
            col("frame_number"),
            col("detection.tracker_id").alias("tracker_id"),
            col("detection.class_id").alias("class_id"),
            col("detection.confidence").alias("confidence"),
            col("detection.bbox").alias("bbox")
        ]
        
        # Add optional fields if they exist
        optional_fields = ["direction", "lane", "vehicle_color", "entry_time", "exit_time"]
        for field in optional_fields:
            if f"detection.{field}" in df.columns:
                detection_fields.append(col(f"detection.{field}").alias(field))
        
        df = df.select(*detection_fields)
        
        # Apply common preprocessing
        df = handle_null_values(df, config["default_values"])
        df = filter_confidence(df, config["confidence_threshold"])
        df = clean_string_columns(df)
        
        # Add bbox coordinates as separate columns
        bbox_schema = ArrayType(DoubleType())
        df = df.withColumn("bbox", col("bbox").cast(bbox_schema))
        df = df.withColumn("bbox_x1", col("bbox")[0]) \
               .withColumn("bbox_y1", col("bbox")[1]) \
               .withColumn("bbox_x2", col("bbox")[2]) \
               .withColumn("bbox_y2", col("bbox")[3])
        
        return df
        
    except Exception as e:
        logging.error(f"Error processing frame data: {e}")
        raise