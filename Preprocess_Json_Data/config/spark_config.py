import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from .minio_config import MINIO_CONFIG

# Load environment variables
load_dotenv()


def create_spark_session(app_name=None):
    app_name = app_name or os.getenv("SPARK_APP_NAME")
    return SparkSession.builder \
        .appName(app_name) \
        .master(os.getenv("SPARK_MASTER", "local[*]")) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"]) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
        .config("spark.hadoop.fs.s3a.buffer.dir", "D:/tmp") \
        .config("spark.hadoop.fs.s3a.committer.name", "magic") \
        .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "D:/tmp") \
        .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace") \
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .config("spark.sql.sources.commitProtocolClass",
                "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
        .config("mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.python.worker.memory", "2g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.python.worker.memory", "2g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.default.parallelism", "100") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.network.timeout", "300s") \
        .config("spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled", "false") \
        .getOrCreate()
