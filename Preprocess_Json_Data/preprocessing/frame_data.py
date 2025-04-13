# from pyspark.sql import DataFrame
# from pyspark.sql.functions import col, explode, struct, lit
# from typing import Dict, Optional
# import logging
# from preprocessing.common import *

# def process_frame_data(df: DataFrame, config: Optional[Dict] = None) -> DataFrame:
#     """
#     Process frame-based detection data with schema:
#     {
#         "frame_number": int,
#         "detections": [
#             {
#                 "tracker_id": int,
#                 "class_id": int,
#                 "confidence": float,
#                 "bbox": [float, float, float, float],
#                 ...
#             }
#         ]
#     }
#     """
#     # Default configuration
#     default_config = {
#         "required_fields": ["frame_number", "detections"],
#         "confidence_threshold": 0.7,
#         "default_values": {
#             "confidence": 0.5,
#             "tracker_id": -1,
#             "class_id": -1
#         }
#     }
    
#     # Merge with user config
#     config = {**default_config, **(config or {})}
    
#     try:
#         # Validate input schema
#         required_fields = config["required_fields"]
#         for field in required_fields:
#             if field not in df.columns:
#                 raise ValueError(f"Missing required field: {field}")
        
#         # Explode detections array
#         df = df.withColumn("detection", explode("detections"))
        
#         # Select and flatten detection fields
#         detection_fields = [
#             col("frame_number"),
#             col("detection.tracker_id").alias("tracker_id"),
#             col("detection.class_id").alias("class_id"),
#             col("detection.confidence").alias("confidence"),
#             col("detection.bbox").alias("bbox")
#         ]
        
#         # Add optional fields if they exist
#         optional_fields = ["direction", "lane", "vehicle_color", "entry_time", "exit_time"]
#         for field in optional_fields:
#             if f"detection.{field}" in df.columns:
#                 detection_fields.append(col(f"detection.{field}").alias(field))
        
#         df = df.select(*detection_fields)
        
#         # Apply common preprocessing
#         df = handle_null_values(df, config["default_values"])
#         df = filter_confidence(df, config["confidence_threshold"])
#         df = clean_string_columns(df)
        
#         # Add bbox coordinates as separate columns
#         bbox_schema = ArrayType(DoubleType())
#         df = df.withColumn("bbox", col("bbox").cast(bbox_schema))
#         df = df.withColumn("bbox_x1", col("bbox")[0]) \
#                .withColumn("bbox_y1", col("bbox")[1]) \
#                .withColumn("bbox_x2", col("bbox")[2]) \
#                .withColumn("bbox_y2", col("bbox")[3])
        
#         return df
        
#     except Exception as e:
#         logging.error(f"Error processing frame data: {e}")
#         raise



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
            StructField("exit_time", StringType(), True)
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
            "speed": 0.0
        }
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
                col("detection.tracker_id").alias("tracker_id"),
                col("detection.class_id").alias("class_id"),
                col("detection.class_name").alias("class_name"),
                col("detection.confidence").alias("confidence"),
                col("detection.bbox").alias("bbox"),
                col("detection.direction").alias("direction"),
                col("detection.lane").alias("lane"),
                col("detection.vehicle_color").alias("vehicle_color"),
                col("detection.stopped").alias("stopped"),
                col("detection.speed").alias("speed"),
                col("detection.entry_time").alias("entry_time"),
                col("detection.exit_time").alias("exit_time")
            )
        )
        
        # Handle null values
        processed_df = handle_null_values(processed_df, config["default_values"])
        
        # Filter by confidence threshold
        processed_df = filter_confidence(processed_df, config["confidence_threshold"])
        
        # Clean string columns
        processed_df = clean_string_columns(processed_df)
        
        # Extract bbox coordinates
        bbox_schema = ArrayType(DoubleType())
        processed_df = (
            processed_df
            .withColumn("bbox", col("bbox").cast(bbox_schema))
            .withColumn("bbox_x1", col("bbox")[0])
            .withColumn("bbox_y1", col("bbox")[1])
            .withColumn("bbox_x2", col("bbox")[2])
            .withColumn("bbox_y2", col("bbox")[3])
        )
        
        # Reconstruct the array of detections per frame
        processed_df = (
            processed_df
            .groupBy("frame_number", "congestion_level")
            .agg(
                collect_list(
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
                        col("direction"),
                        col("lane"),
                        col("vehicle_color"),
                        col("stopped"),
                        col("speed"),
                        col("entry_time"),
                        col("exit_time")
                    )
                ).alias("detections")
            )
        )
        
        # Validate output schema
        for field in FRAME_DATA_SCHEMA.fields:
            if field.name not in processed_df.columns:
                processed_df = processed_df.withColumn(field.name, lit(None).cast(field.dataType))
        
        return processed_df.select([field.name for field in FRAME_DATA_SCHEMA.fields])
        
    except Exception as e:
        logging.error(f"Error processing frame data: {e}")
        raise