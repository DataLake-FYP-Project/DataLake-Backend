from pyspark.sql import SparkSession

def create_spark_session(app_name="DetectionProcessing"):
    return SparkSession.builder \
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