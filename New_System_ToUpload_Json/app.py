from datetime import datetime
import json
import logging
import os
import tempfile
import pandas as pd
from pyspark.sql import functions as F
import streamlit as st
from config.minio_config import BUCKETS
from connectors.minio_connector import MinIOConnector
from config.spark_config import create_spark_session
from pyspark.sql import SparkSession
from process_scripts.common import get_common_output_structure, save_processed_json_to_minio, save_refined_json_to_minio, upload_to_elasticsearch
import processors_registry

# python -m streamlit run app.py
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

st.title("MinIO JSON Upload, Process & Viewer")
type_options = list(processors_registry.PROCESSOR_REGISTRY.keys())
selected_type = st.selectbox("Select data type", type_options, index=type_options.index("vehicle"))

config = processors_registry.PROCESSOR_REGISTRY[selected_type]
processor_class = config["processor_class"]
process_func = config["process_func"]
folder_prefix = config["folder_prefix"]
raw_bucket_name = BUCKETS["raw"]
processed_bucket_name = BUCKETS["processed"]
refine_bucket_name = BUCKETS["refine"]
ELK_index = config["ELK_index"]


data_processor = processor_class(spark)
uploaded_file = st.file_uploader("Upload a JSON file", type=["json"])

def upload_file_to_minio(bucket, file_path, object_name):
    with open(file_path, "rb") as f:
        minio_connector.minio_client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=f,
            length=-1,
            part_size=10*1024*1024
        )

if uploaded_file:
    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename, ext = os.path.splitext(uploaded_file.name)
    object_name = f"{folder_prefix}/{filename}_{now}{ext}"
    st.info(f"Uploading file `{uploaded_file.name}` to raw bucket `{raw_bucket_name}`...")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
        tmp.write(uploaded_file.getbuffer())
        tmp_path = tmp.name

    try:
        upload_file_to_minio(raw_bucket_name, tmp_path, object_name)
        st.success(f"Uploaded to raw bucket `{raw_bucket_name}/{object_name}` successfully.")
    except Exception as e:
        st.error(f"Failed to upload to raw bucket: {e}")

    try:
        raw_df = minio_connector.read_json(raw_bucket_name, object_name)


    except Exception as e:
        st.error(f"Failed to read raw JSON from MinIO: {e}")
        raw_df = None

    if raw_df is not None:
        try:
            processed_result = process_func(raw_df)
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
            processed_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            processed_filename, processed_ext = os.path.splitext(uploaded_file.name)
            processed_object_name = f"{folder_prefix}/processed_{processed_filename}_{processed_now}{processed_ext}"
            success_processed = save_processed_json_to_minio(
                processed_df,
                minio_connector,
                processed_bucket_name,
                processed_object_name,
                wrapped=False
            )

            if success_processed:
                st.success(f"Processed data uploaded to '{processed_bucket_name}' bucket as processed_{uploaded_file.name}")
            else:
                st.error(f"Failed to save or upload processed JSON.")

        except Exception as e:
            st.error(f"Failed to save or upload processed JSON: {e}")

    refined_df = None
    if processed_df is not None:
        try:
            processed_df = data_processor.format_processed_data(processed_df)
            if "timestamp" in processed_df.columns:
                processed_df = processed_df.withColumn("timestamp",
                                            F.to_timestamp(F.regexp_replace("timestamp", r"\+05:30$", "")))
            
            if "frame_timestamp" in processed_df.columns:
                processed_df = processed_df.withColumn("frame_timestamp",
                                            F.to_timestamp(
                                                F.regexp_replace("frame_timestamp", r"\+05:30$", "")))

            grouped = data_processor._group_data(processed_df)
            collected = grouped.collect()

            enriched_data = dict(
                sorted(
                    [data_processor._enrich_data(row) for row in collected],
                    key=lambda x: int(x[0])
                )
            )
            output = get_common_output_structure(uploaded_file.name)
            output.update({
                "count": len(enriched_data),
                "detections": enriched_data
            })
            
            pdf = pd.DataFrame([output])
            refined_df = spark.createDataFrame(pdf)

            if isinstance(refined_df, pd.DataFrame):
                st.write("### Refined Data Sample")
                st.dataframe(refined_df)
                refined_df = spark.createDataFrame(refined_df)
            elif hasattr(refined_df, 'limit'):
                st.write("### Refined Data Sample")
                st.dataframe(refined_df.limit(10).toPandas())

        except Exception as e:
            st.error(f"Error refining processed data: {e}")
            refined_df = None
    else:
        st.info("Processed data not available to refine.")

    if refined_df:
        try:
            refined_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            refined_filename, refined_ext = os.path.splitext(uploaded_file.name)
            refined_object_name = f"{folder_prefix}/refine_{refined_filename}_{refined_now}{refined_ext}"
            success_refined = save_refined_json_to_minio(
                refined_df,
                minio_connector,
                refine_bucket_name,
                refined_object_name,
                wrapped=True
            )

            if success_refined:
                st.success(f"Refined data uploaded to {refine_bucket_name} as {refined_object_name}")
            else:
                st.error("Failed to upload refined JSON.")
        except Exception as e:
            st.error(f"Failed to save or upload refined JSON: {e}")

    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
            json.dump(output, temp_file, indent=4)
            temp_file_path = temp_file.name
        logging.info(f"Saved refined data to temporary file: {temp_file_path}")

        logging.info("Uploading refined JSON to Elasticsearch")
        try:
            upload_to_elasticsearch(temp_file_path, ELK_index)
            logging.info("Successfully uploaded to Elasticsearch")
            st.success("Refined JSON uploaded to Elasticsearch successfully.")
        except Exception as e:
            logging.error(f"Error uploading to Elasticsearch: {str(e)}", exc_info=True)
            st.error(f"Error uploading refined JSON to Elasticsearch: {str(e)}")
    except Exception as e:
        logging.error(f"Error during saving refined JSON to temporary file: {str(e)}", exc_info=True)
        st.error(f"Error during saving refined JSON to temporary file: {str(e)}")
