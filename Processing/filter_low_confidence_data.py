from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

#  Create Spark session with correct configurations
spark = SparkSession.builder \
    .appName("VehicleDataProcessing") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
    .config("spark.hadoop.fs.s3a.buffer.dir", "D:/tmp") \
    .config("spark.hadoop.fs.s3a.committer.name", "magic") \
    .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "D:/tmp") \
    .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace") \
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
    .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
    .config("mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .getOrCreate()

#  Read JSON from MinIO (ensure the file exists)
raw_data = spark.read.option("multiline", "true").json("s3a://raw/vehicle_detection/vehicle_tracking.json")

#  Explode detections (handling array fields correctly)
exploded_data = raw_data.withColumn("detection", explode("detections"))

# Extract relevant fields safely
processed_data = exploded_data.select(
    col("frame_number"),  # Extract frame number
    col("detection.tracker_id").alias("tracker_id"),  # Extract tracker ID
    col("detection.class_id").alias("class_id"),  # Extract class ID
    col("detection.confidence").alias("confidence"),  # Extract confidence score
    col("detection.bbox").alias("bounding_box")  # Extract bounding box coordinates
)


#  Filter out low-confidence detections (e.g., below 0.7)
filtered_data = processed_data.filter(col("confidence") > 0.7)

#  Show results for verification
filtered_data.show(truncate=False)

#  Save processed data to MinIO in Parquet format
filtered_data.write.mode("overwrite").parquet("s3a://processed/vehicle_detection/filter_confidence_vehicle_tracking.parquet")

#  Stop Spark session
spark.stop()