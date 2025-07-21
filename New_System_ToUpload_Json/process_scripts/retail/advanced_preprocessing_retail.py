from collections import Counter, defaultdict
from datetime import datetime
import statistics
from pyspark.sql.functions import col, explode, count, min as spark_min, max as spark_max, collect_list, avg, first, expr
import pyspark.sql.functions as F

class RetailProcessor:
    def __init__(self, spark):
        self.spark = spark

    def format_processed_data(self, df):
        df = df.select(
            col("frame_number"),
            col("timestamp").alias("frame_timestamp"),
            explode("products_detected").alias("product")
        )

        df = df.select(
            "frame_number",
            "frame_timestamp",
            col("product.product_id").alias("product_id"),
            col("product.product_name").alias("product_name"),
            col("product.category").alias("category"),
            col("product.location").alias("location"),
            col("product.stock_level").alias("stock_level"),
            col("product.price").alias("price"),
            col("product.picked_by_customer").alias("picked_by_customer"),
            col("product.expiry_date").cast("date").alias("expiry_date")
        ).filter(col("product_id").isNotNull())

        return df

    def _group_data(self, df):
        return df.groupBy("product_id").agg(
            first("product_name", ignorenulls=True).alias("product_name"),
            first("category", ignorenulls=True).alias("category"),
            first("location", ignorenulls=True).alias("location"),
            avg("price").alias("avg_price"),
            avg("stock_level").alias("avg_stock_level"),
            F.max("picked_by_customer").alias("picked_by_customer_flag"),
            collect_list("expiry_date").alias("expiry_dates"),
            count("frame_number").alias("frame_appearances")
        )

    def _enrich_data(self, row):
        expiry_dates = [d.isoformat() for d in row["expiry_dates"] if d] if row["expiry_dates"] else []
        unique_expiry_dates = list(set(expiry_dates))

        return row["product_id"], {
            "product_name": row["product_name"],
            "category": row["category"],
            "location": row["location"],
            "average_price": row["avg_price"] or 0.0,
            "average_stock_level": row["avg_stock_level"] or 0.0,
            "picked_by_customer": row["picked_by_customer_flag"],
            "unique_expiry_dates": unique_expiry_dates,
            "frame_appearances": row["frame_appearances"]
        }