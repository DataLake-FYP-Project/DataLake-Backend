from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, explode, lit, coalesce

# Step 1: Create Spark Session
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

# Step 2: Read JSON with Corrupt Record Handling
raw_data = spark.read.option("multiline", "true") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .json("s3a://raw/vehicle_detection/vehicle_tracking.json")

# Step 3: Explode `detections` Array (Extracting Nested Fields)
exploded_data = raw_data.withColumn("detection", explode("detections"))

# Step 4: Extract & Ensure All Required Fields Exist
required_columns = {
    "frame_number": lit(0),
    "timestamp": current_timestamp(),
    "detection.vehicle_id": lit(0),
    "detection.class_id": lit(0),
    "detection.confidence": lit(0.5),
    "detection.tracker_id": lit(0)
}

# Dynamically Select Existing Fields & Add Missing Ones
selected_columns = []
for col_name, default_value in required_columns.items():
    if col_name in exploded_data.columns:
        selected_columns.append(col(col_name).alias(col_name.split(".")[-1]))
    else:
        selected_columns.append(default_value.alias(col_name.split(".")[-1]))  # Add missing column

processed_data = exploded_data.select(*selected_columns)

# Step 5: Fill Any Remaining NULL Values
filled_data = processed_data.fillna({
    "confidence": 0.5,
    "tracker_id": 0,
    "class_id": 0,
    "vehicle_id": 0
})

# Convert timestamp format and fill NULL timestamps with a static value
filled_data = filled_data.withColumn(
    "timestamp",
    coalesce(to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"), lit("2023-01-01 00:00:00"))
)

# Step 6: Identify & Remove Corrupt Records (Malformed JSON)
# Check if the `_corrupt_record` column exists before filtering
if "_corrupt_record" in filled_data.columns:
    clean_data = filled_data.filter(filled_data["_corrupt_record"].isNull()).drop("_corrupt_record")
else:
    clean_data = filled_data  # No corrupt records to handle

# Step 7: Save Clean Data
# clean_data.write.mode("overwrite").parquet("s3a://processed/vehicle_detection/clean_vehicle_tracking.parquet")
clean_data.write.mode("overwrite").json("s3a://processed/vehicle_detection/clean_vehicle_tracking.json")

# Step 8: Stop Spark Session
spark.stop()