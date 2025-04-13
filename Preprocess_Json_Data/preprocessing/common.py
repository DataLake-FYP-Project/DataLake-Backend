# from pyspark.sql import DataFrame
# from pyspark.sql.functions import col, explode, to_timestamp, lit, coalesce, when
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
# import logging

# def validate_schema(df: DataFrame, expected_schema: StructType) -> DataFrame:
#     """Validate DataFrame schema against expected schema"""
#     for field in expected_schema.fields:
#         if field.name not in df.columns:
#             raise ValueError(f"Missing required column: {field.name}")
#     return df

# def clean_string_columns(df: DataFrame) -> DataFrame:
#     """Trim whitespace from string columns"""
#     string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
#     for col_name in string_cols:
#         df = df.withColumn(col_name, col(col_name).cast(StringType()))
#     return df

# def handle_null_values(df: DataFrame, default_values: dict) -> DataFrame:
#     """Replace null values with defaults"""
#     for col_name, default_val in default_values.items():
#         if col_name in df.columns:
#             df = df.withColumn(col_name, coalesce(col(col_name), lit(default_val)))
#     return df

# def convert_timestamps(df: DataFrame, timestamp_cols: list, format: str = "yyyy-MM-dd HH:mm:ss") -> DataFrame:
#     """Convert string timestamps to timestamp type"""
#     for col_name in timestamp_cols:
#         if col_name in df.columns:
#             df = df.withColumn(col_name, to_timestamp(col(col_name), format))
#     return df

# def filter_confidence(df: DataFrame, threshold: float = 0.7, confidence_col: str = "confidence") -> DataFrame:
#     """Filter records below confidence threshold"""
#     if confidence_col in df.columns:
#         return df.filter(col(confidence_col) >= threshold)
#     return df

# def add_processing_metadata(df: DataFrame) -> DataFrame:
#     """Add processing metadata columns"""
#     from pyspark.sql.functions import current_timestamp
#     return df.withColumn("processing_timestamp", current_timestamp())


from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, lit, coalesce, when, trim, current_timestamp, md5
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
import logging

def validate_schema(df: DataFrame, expected_schema: StructType) -> DataFrame:
    """Validate DataFrame schema against expected schema with type enforcement"""
    for field in expected_schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, lit(None).cast(field.dataType))
        else:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    return df

def clean_string_columns(df: DataFrame) -> DataFrame:
    """Trim whitespace from string columns and handle nulls"""
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col_name in string_cols:
        df = df.withColumn(
            col_name, 
            when(col(col_name).isNotNull(), trim(col(col_name))))
    return df

def handle_null_values(df: DataFrame, default_values: dict) -> DataFrame:
    """Replace null values with defaults with proper type handling"""
    for col_name, default_val in default_values.items():
        if col_name in df.columns:
            df = df.withColumn(
                col_name, 
                coalesce(
                    col(col_name), 
                    lit(default_val).cast(df.schema[col_name].dataType)
                )
            )
    return df

def convert_timestamps(df: DataFrame, timestamp_cols: list, format: str = "yyyy-MM-dd HH:mm:ss") -> DataFrame:
    """Convert string timestamps to timestamp type with error handling"""
    for col_name in timestamp_cols:
        if col_name in df.columns:
            df = df.withColumn(
                col_name, 
                to_timestamp(col(col_name), format)
            )
    return df

def filter_confidence(df: DataFrame, threshold: float = 0.7, confidence_col: str = "confidence") -> DataFrame:
    """Filter records below confidence threshold"""
    if confidence_col in df.columns:
        return df.filter(col(confidence_col) >= threshold)
    return df

def add_processing_metadata(df: DataFrame) -> DataFrame:
    """Add processing metadata columns"""
    return (df
            .withColumn("processing_timestamp", current_timestamp())
            .withColumn("processing_id", md5(current_timestamp().cast("string")))
    )