
from process_scripts.advanced_preprocessing_vehicle import VehicleProcessor
from process_scripts.basic_preprocessing_vehicle import process_vehicle_json_data
from process_scripts.common import get_common_output_structure
import pandas as pd
from pyspark.sql import functions as F
from processors.base_process import BaseProcessor

class VehicleDataProcessor(BaseProcessor):
    def __init__(self, spark):
        super().__init__(spark)
        self.vp = VehicleProcessor(spark)

    def basic_process(self, raw_df):
        result = process_vehicle_json_data(raw_df)
        return result[0] if isinstance(result, tuple) else result

    def refine_process(self, processed_df):
        df = self.vp._process_vehicle_format(processed_df)
        df = df.withColumn("timestamp", F.to_timestamp(F.regexp_replace("timestamp", r"\\+05:30$", "")))
        if "frame_timestamp" in df.columns:
            df = df.withColumn("frame_timestamp", F.to_timestamp(F.regexp_replace("frame_timestamp", r"\\+05:30$", "")))
        return df

    def get_output_json(self, refined_df, filename):
        grouped = self.vp._group_data(refined_df)
        collected = grouped.collect()
        enriched_data = dict(sorted([
            self.vp._enrich_vehicle(row) for row in collected
        ], key=lambda x: int(x[0])))

        output = get_common_output_structure(filename)
        output.update({
            "vehicle_count": len(enriched_data),
            "vehicles": enriched_data
        })
        return output