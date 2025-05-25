import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from .advanced_preprocessing_people import PeopleProcessor
from .advanced_preprocessing_vehicle import VehicleProcessor
from ..config.spark_config import create_spark_session
from ..config.minio_config import BUCKETS
from pyspark.sql import functions as F


class CombinedProcessor:
    def __init__(self, spark, detection_type):
        self.spark = spark
        self.people_processor = PeopleProcessor(spark)
        self.vehicle_processor = VehicleProcessor(spark)

    def _get_common_output_structure(self, source_file):
        """Common output structure for both people and vehicle processing"""
        return {
            "source_file": source_file,
            "processing_date": datetime.now(timezone.utc).isoformat(),
            "processing_version": "1.0"
        }

    def _write_output(self, output_bucket, out_path, output_data):
        """Common output writing functionality"""
        self.people_processor.minio.write_single_json(output_data, output_bucket, out_path)

    # def _process_file(self, processor, input_bucket, output_bucket, file, prefix):
    #     """Common file processing logic"""

    #     logging.info(f"Processing {file}...")
    #     try:
    #         df = processor.minio.read_json(input_bucket, f"{prefix}/{file}")

    #         if processor.__class__.__name__ == "PeopleProcessor":
    #             if "frame_detections" in df.columns:
    #                 processed_df = processor._process_frame_detections_format(df)
    #             elif "detections" in df.columns:
    #                 processed_df = processor._process_flat_detections_format(df)
    #             else:
    #                 logging.info(f"Skipping {file} - unknown format")
    #                 return None
    #         else:  # VehicleProcessor
    #             processed_df = processor._process_vehicle_format(df)

    #         # Common timestamp processing
    #         processed_df = processed_df.withColumn("timestamp",
    #                                                F.to_timestamp(F.regexp_replace("timestamp", r"\+05:30$", ""))
    #                                                )

    #         if "frame_timestamp" in processed_df.columns:
    #             processed_df = processed_df.withColumn("frame_timestamp",
    #                                                    F.to_timestamp(
    #                                                        F.regexp_replace("frame_timestamp", r"\+05:30$", ""))
    #                                                    )

    #         return processed_df
    #     except Exception as e:
    #         logging.info(f"Error processing file {file}: {str(e)}")
    #         return None

    def _process_file(self, processor, input_bucket, output_bucket, file, prefix):
        """Common file processing logic"""
        logging.info(f"Processing {file}...")
        try:
            df = processor.minio.read_json(input_bucket, f"{prefix}/{file}")
            if df is None:
                logging.info("Failed to read input file")
                return None

            # Early check for all tracker_ids being -1
            if "tracker_id" in df.columns:
                valid_count = df.filter(F.col("tracker_id") != -1).count()
                if valid_count == 0:
                    logging.info(f"Stopping preprocessing - all tracker IDs are -1 in file {file}")
                    return "all_invalid"  # Special return value
            
            if processor.__class__.__name__ == "PeopleProcessor":
                if "frame_detections" in df.columns:
                    processed_df = processor._process_frame_detections_format(df)
                elif "detections" in df.columns:
                    processed_df = processor._process_flat_detections_format(df)
                else:
                    logging.info(f"Skipping {file} - unknown format")
                    return None
            else:  # VehicleProcessor
                processed_df = processor._process_vehicle_format(df)

            if processed_df is None or processed_df.count() == 0:
                logging.info(f"No valid detections found in {file}")
                return None

            # Common timestamp processing
            processed_df = processed_df.withColumn("timestamp",
                                            F.to_timestamp(F.regexp_replace("timestamp", r"\+05:30$", "")))
            
            if "frame_timestamp" in processed_df.columns:
                processed_df = processed_df.withColumn("frame_timestamp",
                                                F.to_timestamp(
                                                    F.regexp_replace("frame_timestamp", r"\+05:30$", "")))
            return processed_df
        except Exception as e:
            logging.info(f"Error processing file {file}: {str(e)}")
            return None
    

    # def process_all(self, input_bucket: str, output_bucket: str, detection_type,filename):
    #     """Process both people and vehicle detection files with error handling"""
    #     start_time = datetime.now(timezone.utc)

    #     if detection_type == "People":
    #         # Process people detections
    #         logging.info("Processing People Detections")
    #         try:
    #             people_file = self.people_processor.minio.get_json_file(input_bucket, f"people_detection/preprocessed_{filename}")
    #             processed_df = self._process_file(self.people_processor, input_bucket, output_bucket, people_file,
    #                                                 "people_detection")
    #             if processed_df:
    #                 grouped = self.people_processor._group_data(processed_df)
    #                 enriched_data = dict(
    #                     sorted([self.people_processor._enrich_person(row) for row in grouped.collect()],
    #                             key=lambda x: int(x[0])))
    #                 output = self._get_common_output_structure(people_file)
    #                 output.update({
    #                     "people_count": len(enriched_data),
    #                     "people": enriched_data
    #                 })
    #                 out_path = f"people_detection/refine_{filename}"
    #                 self._write_output(output_bucket, out_path, output)
    #                 logging.info(f"Successfully processed {len(enriched_data)} people in {people_file}")
    #                 logging.info(f"Successfully wrote output to refined/{out_path}")
    #         except Exception as e:
    #             logging.info(f"ERROR in people processing: {str(e)}")
    #             logging.info("Continuing with vehicle processing...")

    #     elif detection_type == "Vehicle":
    #         # Process vehicle detections
    #         logging.info("Processing Vehicle Detections")
    #         try:
    #             vehicle_file = self.vehicle_processor.minio.get_json_file(input_bucket, f"vehicle_detection/preprocessed_{filename}")
    #             processed_df = self._process_file(self.vehicle_processor, input_bucket, output_bucket, vehicle_file,
    #                                                 "vehicle_detection")
    #             if processed_df:
    #                 grouped = self.vehicle_processor._group_data(processed_df)
    #                 enriched_data = dict(
    #                     sorted([self.vehicle_processor._enrich_vehicle(row) for row in grouped.collect()],
    #                             key=lambda x: int(x[0])))
    #                 output = self._get_common_output_structure(vehicle_file)
    #                 output.update({
    #                     "vehicle_count": len(enriched_data),
    #                     "vehicles": enriched_data
    #                 })
    #                 out_path = f"vehicle_detection/refine_{filename}"
    #                 self._write_output(output_bucket, out_path, output)
    #                 logging.info(f"Successfully processed {len(enriched_data)} vehicles in {vehicle_file}")
    #                 logging.info(f"Successfully wrote output to refined/{out_path}")
    #         except Exception as e:
    #             logging.info(f"ERROR in vehicle processing: {str(e)}")
    #             logging.info("Processing completed with errors")

    #     end_time = datetime.now(timezone.utc)
    #     duration = (end_time - start_time).total_seconds()
    #     logging.info(f"Advanced Processing completed in {duration:.2f} seconds")

    def process_all(self, input_bucket: str, output_bucket: str, detection_type, filename):
        """Process detection files and stop if all tracker IDs are invalid"""
        start_time = datetime.now(timezone.utc)
        
        if detection_type == "People":
            logging.info("Processing People Detections")
            try:
                people_file = self.people_processor.minio.get_json_file(input_bucket, f"people_detection/preprocessed_{filename}")
                processed_df = self._process_file(self.people_processor, input_bucket, output_bucket, people_file,
                                                "people_detection")
                
                if processed_df == "all_invalid":
                    logging.info("Stopping preprocessing - all people tracker IDs are -1")
                    return -1
                    
                if processed_df is None:
                    logging.info("No valid people detections to process")
                    return -1
                    
                grouped = self.people_processor._group_data(processed_df)
                collected = grouped.collect()
                enriched_data = dict(
                    sorted([self.people_processor._enrich_person(row) for row in collected],
                        key=lambda x: int(x[0])))
                
                output = self._get_common_output_structure(people_file)
                output.update({
                    "people_count": len(enriched_data),
                    "people": enriched_data
                })
                out_path = f"people_detection/refine_{filename}"
                self._write_output(output_bucket, out_path, output)
                logging.info(f"Successfully processed {len(enriched_data)} people in {people_file}")
                
            except Exception as e:
                logging.info(f"ERROR in people processing: {str(e)}")
                return -1

        elif detection_type == "Vehicle":
            logging.info("Processing Vehicle Detections")
            try:
                vehicle_file = self.vehicle_processor.minio.get_json_file(input_bucket, f"vehicle_detection/preprocessed_{filename}")
                processed_df = self._process_file(self.vehicle_processor, input_bucket, output_bucket, vehicle_file,
                                                "vehicle_detection")
                
                if processed_df == "all_invalid":
                    logging.info("Stopping preprocessing - all vehicle tracker IDs are -1")
                    return -1
                    
                if processed_df is None:
                    logging.info("No valid vehicle detections to process")
                    return -1
                    
                grouped = self.vehicle_processor._group_data(processed_df)
                collected = grouped.collect()
                enriched_data = dict(
                    sorted([self.vehicle_processor._enrich_vehicle(row) for row in collected],
                        key=lambda x: int(x[0])))
                
                output = self._get_common_output_structure(vehicle_file)
                output.update({
                    "vehicle_count": len(enriched_data),
                    "vehicles": enriched_data
                })
                out_path = f"vehicle_detection/refine_{filename}"
                self._write_output(output_bucket, out_path, output)
                logging.info(f"Successfully processed {len(enriched_data)} vehicles in {vehicle_file}")
                
            except Exception as e:
                logging.info(f"ERROR in vehicle processing: {str(e)}")
                return -1

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        logging.info(f"Advanced Processing completed in {duration:.2f} seconds")
        return 1



def advanced_preprocessing(detection_type,filename):
    spark = create_spark_session()
    try:
        processor = CombinedProcessor(spark, detection_type)
        processing_status=processor.process_all(BUCKETS["processed"], BUCKETS["refine"], detection_type,filename)
        return processing_status
    except Exception as e:
        logging.info(f"Fatal error in combined processing: {str(e)}")
        return -1
    finally:
        spark.stop()
