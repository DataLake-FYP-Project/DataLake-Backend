from pyspark.sql import SparkSession
from config.spark_config import create_spark_session
from config.minio_config import BUCKETS
from connectors.minio_connector import MinIOConnector
from preprocessing.frame_data import process_frame_data
from preprocessing.tracking_data import process_tracking_data
import logging

def main():
    # Initialize Spark with legacy time parser
    spark = create_spark_session()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    
    minio_conn = MinIOConnector(spark)
    
    try:
        # Process frame-based data
        frame_df = minio_conn.read_json(BUCKETS["raw"], "detections/Video_01_frame_data.json", multiLine=True)
        processed_frame_df = process_frame_data(frame_df, {
            "confidence_threshold": 0.7,
            "default_values": {
                "confidence": 0.5,
                "tracker_id": -1,
                "class_id": -1
            }
        })
        minio_conn.write_json(
            processed_frame_df, 
            BUCKETS["processed"], 
            "detections/processed_video_01_frame_data/"
        )
        
        # Process tracking-based data
        tracking_df = minio_conn.read_json(
            BUCKETS["raw"], 
            "detections/people_video_frame_data.json",
            multiLine=True
        )
        
        processed_tracking_df = process_tracking_data(tracking_df, {
            "timestamp_format": "yyyy-MM-dd HH:mm:ss",
            "default_values": {
                "age": -1,
                "mask_confidence": 0.0,
                "confidence": 0.5,
                "gender": "Unknown"
            }
        })
        
        minio_conn.write_json(
            processed_tracking_df,
            BUCKETS["processed"],
            "detections/processed_people_video_frame_data/"
        )
        
        logging.info("Processing completed successfully")
        
    except Exception as e:
        logging.error(f"Processing failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()