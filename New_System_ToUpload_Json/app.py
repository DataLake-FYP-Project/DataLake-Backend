import glob
import os
import streamlit as st
import tempfile
import json
from process_scripts.basic_preprocessing_vehicle import process_vehicle_json_data
from connectors.minio_connector import MinIOConnector
from config.spark_config import create_spark_session
from pyspark.sql import SparkSession

st.set_page_config(page_title="MinIO Spark Viewer", layout="wide")

@st.cache_resource
def get_spark() -> SparkSession:
    return create_spark_session()

@st.cache_resource
def get_minio_connector() -> MinIOConnector:
    spark = get_spark()
    return MinIOConnector(spark)

minio_connector = get_minio_connector()

st.title("MinIO JSON Upload, Process & Viewer")

# Upload JSON file via Streamlit
uploaded_file = st.file_uploader("Upload a JSON file", type=["json"])

raw_bucket_name = "raw"
processed_bucket_name = "processed"

import logging


def save_processed_json_to_minio(processed_df, minio_connector, bucket_name, upload_filename, wrapped=True):
    try:
        clean_path = upload_filename.lstrip('/')

        if wrapped:
            # If your MinIOConnector supports writing wrapped JSON
            minio_connector.write_wrapped_json(processed_df, bucket_name, clean_path, key="frame_detections")
            print("wrappedddd")
        else:
            # Write normal JSON
            minio_connector.write_json(processed_df, bucket_name, clean_path)

        logging.info(f"Successfully uploaded to {bucket_name}/{clean_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to upload processed JSON to {bucket_name}/{upload_filename}: {e}")
        return False


def upload_file_to_minio(bucket, file_path, object_name):
    with open(file_path, "rb") as f:
        minio_connector.minio_client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=f,
            length=-1,  # length unknown (streaming)
            part_size=10*1024*1024  # 10MB part size
        )

if uploaded_file:
    st.info(f"Uploading file `{uploaded_file.name}` to raw bucket `{raw_bucket_name}`...")
    
    # Save uploaded file temporarily
    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
        tmp.write(uploaded_file.getbuffer())
        tmp_path = tmp.name
    
    # Upload raw JSON file to MinIO raw bucket
    try:
        upload_file_to_minio(raw_bucket_name, tmp_path, uploaded_file.name)
        st.success(f"Uploaded to raw bucket `{raw_bucket_name}` successfully.")
    except Exception as e:
        st.error(f"Failed to upload to raw bucket: {e}")

    # Read raw JSON file from MinIO into Spark DataFrame
    try:
        raw_df = minio_connector.read_json(raw_bucket_name, uploaded_file.name)
        st.write("### Raw Data Sample")
        st.dataframe(raw_df.limit(10).toPandas())
    except Exception as e:
        st.error(f"Failed to read raw JSON from MinIO: {e}")
        raw_df = None

    # Process the DataFrame
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
            st.error(f"Error processing data: {e}")
            processed_df = None
    else:
        processed_df = None

    print(processed_df)

    # Save processed data to JSON locally then upload to processed bucket
    if processed_df:
        try:
            success = save_processed_json_to_minio(
                processed_df,
                minio_connector,
                processed_bucket_name,
                f"processed_{uploaded_file.name}",
                wrapped=True  # Set this to False if you don’t want wrapping
            )

            if success:
                st.success(f"Processed data uploaded to '{processed_bucket_name}' bucket as processed_{uploaded_file.name}")
            else:
                st.error(f"Failed to save or upload processed JSON.")

            st.success(f"Processed data uploaded to '{processed_bucket_name}' bucket as processed_{uploaded_file.name}")
        except Exception as e:
            st.error(f"Failed to save or upload processed JSON: {e}")
