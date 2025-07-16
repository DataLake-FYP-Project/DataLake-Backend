from pyspark.sql.functions import col, explode, struct, collect_list, size
from pyspark.sql.types import *
from process_scripts.common import clean_string_columns, convert_timestamps, handle_null_values

def process_retail_json_data(df):
    required_columns = ["frame_number", "timestamp", "detections"]
    missing_columns = [c for c in required_columns if c not in df.columns]

    if missing_columns:
        print(f"Missing required columns: {missing_columns}. Skipping processing.")
        return df, -1

    default_config = {
        "default_values": {
            "product_id": "unknown",
            "product_name": "unknown",
            "category": "unknown",
            "location": "unknown",
            "stock_level": 0,
            "price": 0.0,
            "picked_by_customer": False,
            "expiry_date": None
        },
        "timestamp_fields": ["timestamp", "expiry_date"],
        "preserve_null_fields": ["timestamp", "expiry_date"]
    }

    config = default_config | {}

    try:
        total_products = df.select(size(col("detections")).alias("count")).agg({"count": "sum"}).collect()[0][0]
        if total_products == 0:
            print("No products detected. Returning original DataFrame.")
            return df, -1

        frame_columns = [c for c in ["frame_number", "timestamp", "detections"] if c in df.columns]

        processed = df.withColumn("product", explode("detections")) \
            .select(
                *[col(c) for c in frame_columns],
                col("product.*")
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

        product_fields = []
        possible_fields = [
            ("product_id", "product_id"),
            ("product_name", "product_name"),
            ("category", "category"),
            ("location", "location"),
            ("stock_level", "stock_level"),
            ("price", "price"),
            ("picked_by_customer", "picked_by_customer"),
            ("expiry_date", "expiry_date")
        ]

        for field_name, col_name in possible_fields:
            if col_name in processed.columns:
                product_fields.append(col(col_name).alias(field_name))

        reconstructed_products = processed.select(
            *[col(c) for c in frame_columns],
            struct(*product_fields).alias("product")
        )

        result = (
            reconstructed_products
            .groupBy(*frame_columns)
            .agg(collect_list("product").alias("detections"))
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
        print(f"Error processing retail product detection data: {str(e)}")
        return df, -1