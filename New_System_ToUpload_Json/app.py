from datetime import datetime, timezone
import glob
from io import BytesIO
import os
import pandas as pd
import streamlit as st
import tempfile
import json
import logging
from process_scripts.advanced_preprocessing_vehicle import VehicleProcessor
from process_scripts.basic_preprocessing_vehicle import process_vehicle_json_data
from connectors.minio_connector import MinIOConnector
from config.spark_config import create_spark_session
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from typing import Dict, Any, List, Optional

st.set_page_config(page_title="MinIO Spark Viewer", layout="wide")

@st.cache_resource
def get_spark() -> SparkSession:
    return create_spark_session()

@st.cache_resource
def get_minio_connector() -> MinIOConnector:
    spark = get_spark()
    return MinIOConnector(spark)

spark = get_spark()
minio_connector = get_minio_connector()
vehicle_processor = VehicleProcessor(spark)  # initialize processor

st.title("MinIO JSON Upload, Process & Viewer")

uploaded_file = st.file_uploader("Upload a JSON file", type=["json"])

raw_bucket_name = "raw"
processed_bucket_name = "processed"
refine_bucket_name = "refine"


def save_processed_json_to_minio(processed_df, minio_connector, bucket_name, upload_filename, wrapped=False):
    try:
        clean_path = upload_filename.lstrip('/')

        if wrapped:
            minio_connector.write_wrapped_json(processed_df, bucket_name, clean_path, key="frame_detections")
        else:
            minio_connector.write_json(processed_df, bucket_name, clean_path)

        logging.info(f"Successfully uploaded processed JSON to {bucket_name}/{clean_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to upload processed JSON to {bucket_name}/{upload_filename}: {e}")
        return False

    
def save_refined_json_to_minio(refined_df, minio_connector, bucket_name, upload_filename, wrapped=True):
    try:
        clean_path = upload_filename.lstrip('/')
        minio_connector.write_wrapped_json(refined_df, refine_bucket_name, clean_path, key="frame_detections")

        logging.info(f"Successfully uploaded refined JSON to {bucket_name}/{clean_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to upload refined JSON to {bucket_name}/{upload_filename}: {e}")
        return False


def upload_file_to_minio(bucket, file_path, object_name):
    with open(file_path, "rb") as f:
        minio_connector.minio_client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=f,
            length=-1,
            part_size=10*1024*1024
        )

def get_common_output_structure(source_file):
        """Common output structure for both people and vehicle processing"""
        return {
            "source_file": source_file,
            "processing_date": datetime.now(timezone.utc).isoformat(),
            "processing_version": "1.0"
        }


if uploaded_file:
    st.info(f"Uploading file `{uploaded_file.name}` to raw bucket `{raw_bucket_name}`...")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
        tmp.write(uploaded_file.getbuffer())
        tmp_path = tmp.name

    try:
        upload_file_to_minio(raw_bucket_name, tmp_path, uploaded_file.name)
        st.success(f"Uploaded to raw bucket `{raw_bucket_name}` successfully.")
    except Exception as e:
        st.error(f"Failed to upload to raw bucket: {e}")

    try:
        raw_df = minio_connector.read_json(raw_bucket_name, uploaded_file.name)
        st.write("### Raw Data Sample")
        st.dataframe(raw_df.limit(10).toPandas())
    except Exception as e:
        st.error(f"Failed to read raw JSON from MinIO: {e}")
        raw_df = None

    if raw_df is not None:
        try:
            processed_result = process_vehicle_json_data(raw_df)
            if isinstance(processed_result, tuple):
                processed_df = processed_result[0]
            else:
                processed_df = processed_result
            if processed_df:
                st.write("### Processed Data Sample")
                st.dataframe(processed_df.limit(10).toPandas())
            else:
                st.info("No processed data generated.")
        except Exception as e:
            st.error(f"Error processing raw data: {e}")
            processed_df = None
    else:
        processed_df = None

    if processed_df:
        try:
            success_processed = save_processed_json_to_minio(
                processed_df,
                minio_connector,
                processed_bucket_name,
                f"processed_{uploaded_file.name}",
                wrapped=False
            )

            if success_processed:
                st.success(f"Processed data uploaded to '{processed_bucket_name}' bucket as processed_{uploaded_file.name}")
            else:
                st.error(f"Failed to save or upload processed JSON.")

            st.success(f"Processed data uploaded to '{processed_bucket_name}' bucket as processed_{uploaded_file.name}")

        except Exception as e:
            st.error(f"Failed to save or upload processed JSON: {e}")


    refined_df = None
    if processed_df is not None:
        try:
            print("judy")
            processed_df = vehicle_processor._process_vehicle_format(processed_df)
            processed_df = processed_df.withColumn("timestamp",
                                            F.to_timestamp(F.regexp_replace("timestamp", r"\+05:30$", "")))
            
            if "frame_timestamp" in processed_df.columns:
                processed_df = processed_df.withColumn("frame_timestamp",
                                                F.to_timestamp(
                                                    F.regexp_replace("frame_timestamp", r"\+05:30$", "")))
            grouped = vehicle_processor._group_data(processed_df)
            collected = grouped.collect()

            enriched_data = dict(
                sorted(
                    [vehicle_processor._enrich_vehicle(row) for row in collected],
                    key=lambda x: int(x[0])
                )
            )

            output = get_common_output_structure(uploaded_file.name)
            output.update({
                "vehicle_count": len(enriched_data),
                "vehicles": enriched_data
            })
            print("Output------",output)

            vehicles_list = []
            for vehicle_id, vehicle_info in enriched_data.items():
                # Flatten each vehicle record, add vehicle_id as a column
                record = {"vehicle_id": vehicle_id}
                record.update(vehicle_info)  # assuming vehicle_info is a dict
                vehicles_list.append(record)

            if vehicles_list:
                vehicles_df = pd.DataFrame(vehicles_list)
                st.write("### Refined Data Sample")
                st.dataframe(vehicles_df)  # Display nice tabular data with each vehicle in one row
            else:
                st.info("No refined data to display.")

            # If you still want the original DataFrame 'refined_df' with one row for export or other use, you can keep it
            pdf = pd.DataFrame([output])
            refined_df = spark.createDataFrame(pdf)
        
        except Exception as e:
            st.error(f"Error refining processed data: {e}")
            refined_df = None
    else:
        st.info("Processed data not available to refine.")

    if refined_df:
        try:
            success_refined = save_refined_json_to_minio(
                refined_df,
                minio_connector,
                refine_bucket_name,
                f"refine_{uploaded_file.name}",
                wrapped=True
            )

            if success_refined:
                st.success(f"refined data uploaded to {refine_bucket_name}' as refine_{uploaded_file.name}")
            else:
                st.error("Failed to upload either refined JSON.")
        except Exception as e:
            st.error(f"Failed to save or upload processed/refined JSON: {e}")
