from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, struct, lit, when, array
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType
from typing import Dict, Optional
import logging
from preprocessing.common import *
from pyspark.sql.functions import collect_list, struct

FRAME_DATA_SCHEMA = StructType([
    StructField("frame_number", IntegerType(), False),
    StructField("congestion_level", IntegerType(), True),
    StructField("detections", ArrayType(
        StructType([
            StructField("tracker_id", IntegerType(), False),
            StructField("class_id", IntegerType(), False),
            StructField("class_name", StringType(), True),
            StructField("confidence", DoubleType(), False),
            StructField("bbox", ArrayType(DoubleType()), False),
            StructField("bbox_x1", DoubleType(), True),
            StructField("bbox_y1", DoubleType(), True),
            StructField("bbox_x2", DoubleType(), True),
            StructField("bbox_y2", DoubleType(), True),
            StructField("direction", StringType(), True),
            StructField("lane", StringType(), True),
            StructField("vehicle_color", StringType(), True),
            StructField("stopped", BooleanType(), True),
            StructField("speed", DoubleType(), True),
            StructField("entry_time", StringType(), True),
            StructField("exit_time", StringType(), True),
            # Add any additional fields from raw data here
            StructField("timestamp", StringType(), True),
            StructField("vehicle_type", StringType(), True)
        ])
    ), True)
])

def process_frame_data(df: DataFrame, config: Optional[Dict] = None) -> DataFrame:
    """Process frame-based detection data with enhanced schema validation"""
    default_config = {
        "required_fields": ["frame_number", "detections"],
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
            "exit_time": None
        },
        "preserve_null_fields": ["entry_time", "exit_time"]  # Fields where null should be preserved
    }
    
    config = {**default_config, **(config or {})}
    
    try:
        # Validate input schema
        for field in config["required_fields"]:
            if field not in df.columns:
                raise ValueError(f"Missing required field: {field}")
        
        # Explode detections array and flatten structure
        processed_df = (
            df
            .withColumn("detection", explode("detections"))
            .select(
                col("frame_number"),
                col("congestion_level"),
                # Include all detection fields
                col("detection.*")  # This gets all fields from the detection struct
            )
        )
        
        # Handle null values - only for fields that need defaults
        for field, default_value in config["default_values"].items():
            if field not in config["preserve_null_fields"]:
                processed_df = processed_df.withColumn(
                    field, 
                    coalesce(col(field), lit(default_value)))
        
        # Filter by confidence threshold
        processed_df = filter_confidence(processed_df, config["confidence_threshold"])
        
        # Clean string columns
        processed_df = clean_string_columns(processed_df)
        
        # Ensure bbox coordinates are properly extracted
        if "bbox" in processed_df.columns:
            processed_df = (
                processed_df
                .withColumn("bbox", col("bbox").cast(ArrayType(DoubleType())))
                .withColumn("bbox_x1", col("bbox")[0])
                .withColumn("bbox_y1", col("bbox")[1])
                .withColumn("bbox_x2", col("bbox")[2])
                .withColumn("bbox_y2", col("bbox")[3])
            )
        
            optional_fields = [
                "tracker_id", "class_id", "class_name", "confidence", "bbox",
                "bbox_x1", "bbox_y1", "bbox_x2", "bbox_y2", "direction", "lane",
                "vehicle_color", "stopped", "speed", "entry_time", "exit_time",
                "timestamp", "vehicle_type"
            ]

            existing_fields = [f for f in optional_fields if f in processed_df.columns]

            columns_to_select = ["frame_number", "congestion_level"] + existing_fields

            return processed_df.select(columns_to_select)


        
    except Exception as e:
        logging.error(f"Error processing frame data: {e}")
        raise

