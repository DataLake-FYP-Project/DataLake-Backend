from pyspark.sql import SparkSession

# Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("ParquetReader").getOrCreate()

# Step 2: Load Parquet File (Modify path as needed)
parquet_path = "s3a://processed/vehicle_detection/clean_vehicle_tracking.parquet/"  # Modify as needed
df = spark.read.parquet(parquet_path)

# Step 3: Display Schema
print("\n Schema of the Parquet File:")
df.printSchema()

# Step 4: Count Total Records
record_count = df.count()
print(f"\n Total Records: {record_count}")

# Step 5: Show Sample Data (First 5 Rows)
print("\n Sample Data:")
df.show(25, truncate=False)

# Step 6: Summary Statistics (Numerical Columns)
print("\n Summary Statistics:")
df.describe().show()

# Step 8: Stop Spark Session
spark.stop()


