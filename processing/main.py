from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, count

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MinIO JSON Processing") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-server:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "your-access-key") \
    .config("spark.hadoop.fs.s3a.secret.key", "your-secret-key") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Read JSON file from MinIO
json_path = "s3a://camera-footage/frame_data.json"
df = spark.read.option("multiline", "true").json(json_path)

# Explode detections array to process each object separately
detections_df = df.select(explode(col("detections")).alias("detection"))

# Extract required fields
tracking_df = detections_df.select(
    col("detection.tracker_id").alias("tracker_id"),
    col("detection.class_id").alias("class_id")
)

# Count occurrences of each tracker_id
tracker_counts = tracking_df.groupBy("tracker_id").count()

# Calculate color percentage per tracking ID
color_percentage_df = tracker_counts.withColumn(
    "color_percentage", (col("count") / tracker_counts.agg(count("tracker_id")).first()[0]) * 100
)

# Show results
color_percentage_df.show()

# Stop Spark session
spark.stop()
