import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
import gc

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
        self.detection_type = detection_type

    def _get_common_output_structure(self, source_file):
        """Common output structure for both people and vehicle processing"""
        return {
            "source_file": source_file,
            "processing_date": datetime.now(timezone.utc).isoformat(),
            "processing_version": "1.0",
            "processing_errors": []
        }

    def _write_output(self, output_bucket, out_path, output_data):
        """Common output writing functionality"""
        try:
            processor = self.people_processor if self.detection_type == "People" else self.vehicle_processor
            processor.minio.write_single_json(output_data, output_bucket, out_path)
            return True
        except Exception as e:
            self.logger.error(f"Failed to write output to {output_bucket}/{out_path}: {str(e)}")
            return False

    def _safe_df_operation(self, df, operation):
        """Wrapper for DataFrame operations with memory management"""
        try:
            gc.collect()
            
            if df is None:
                return None
                
            if hasattr(df, 'rdd') and df.rdd.getNumPartitions() > 10:
                df = df.coalesce(10)
                
            result = operation(df)
            
            if hasattr(df, 'unpersist'):
                df.unpersist()
                
            return result
        except Exception as e:
            self.logger.error(f"Operation failed: {str(e)}", exc_info=True)
            return None

    def _process_people_data(self, df):
        """Process people data with column validation"""
        try:
            if df is None:
                return None
                
            if "frame_detections" in df.columns:
                return self.people_processor._process_frame_detections_format(df)
            elif "detections" in df.columns:
                return self.people_processor._process_flat_detections_format(df)
            else:
                self.logger.error("Unknown people data format - missing both frame_detections and detections columns")
                return None
        except Exception as e:
            self.logger.error(f"Error processing people data: {str(e)}", exc_info=True)
            return None

    def _process_vehicle_data(self, df):
        """Process vehicle data with column validation"""
        try:
            if df is None:
                return None
            return self.vehicle_processor._process_vehicle_format(df)
        except Exception as e:
            self.logger.error(f"Error processing vehicle data: {str(e)}", exc_info=True)
            return None

    def _process_timestamps(self, df):
        """Process timestamps with column validation"""
        try:
            if "timestamp" in df.columns:
                df = df.withColumn("timestamp", 
                                 F.to_timestamp(F.regexp_replace("timestamp", r"\+05:30$", "")))
            
            if "frame_timestamp" in df.columns:
                df = df.withColumn("frame_timestamp",
                                  F.to_timestamp(F.regexp_replace("frame_timestamp", r"\+05:30$", "")))
            return df
        except Exception as e:
            self.logger.error(f"Error processing timestamps: {str(e)}")
            return df

    def _process_file(self, processor, input_bucket, file, prefix):
        """Common file processing logic"""

        logging.info(f"Processing {file}...")
        try:
            df = processor.minio.read_json(input_bucket, f"{prefix}/{file}")
            if df is None:
                self.logger.error("Failed to read input file")
                return None
                
            if processor.__class__.__name__ == "PeopleProcessor":
                processed_df = self._process_people_data(df)
            else:  # VehicleProcessor
                processed_df = self._process_vehicle_data(df)

            if processed_df is None:
                self.logger.error("Failed to process DataFrame")
                return None
            
            # Common timestamp processing
            processed_df = self._process_timestamps(processed_df)
            
            return processed_df
            
        except Exception as e:
            logging.info(f"Error processing file {file}: {str(e)}")
            return None

    def process_all(self, input_bucket: str, output_bucket: str, detection_type,filename):
        """Process both people and vehicle detection files with error handling"""
        start_time = datetime.now(timezone.utc)
        output = None
        max_retries = 2
        retry_count = 0
        
        while retry_count <= max_retries:
            try:
                if detection_type == "People":
                    logging.info("Processing People Detections")
                    processor = self.people_processor
                    prefix = "people_detection"
                    file_key = f"preprocessed_{filename}"
                    output_key = f"people_detection/refine_{filename}"
                else:
                    logging.info("Processing Vehicle Detections")
                    processor = self.vehicle_processor
                    prefix = "vehicle_detection"
                    file_key = f"preprocessed_{filename}"
                    output_key = f"vehicle_detection/refine_{filename}"

                input_file = processor.minio.get_json_file(input_bucket, f"{prefix}/{file_key}")
                if not input_file:
                    raise ValueError(f"File not found: {prefix}/{file_key}")
                    
                processed_df = self._process_file(processor, input_bucket, input_file, prefix)
                
                if processed_df is None:
                    raise ValueError("Failed to process DataFrame")
                    
                grouped = processor._group_data(processed_df)
                if grouped is None:
                    raise ValueError(f"Failed to group {detection_type.lower()} data")
                    
                # Collect results
                collected = grouped.collect()
                
                # Enrich data
                if detection_type == "People":
                    enriched_data = dict(
                        sorted([self.people_processor._enrich_person(row) for row in collected],
                              key=lambda x: int(x[0]) if x[0].isdigit() else x[0]))
                    output = self._get_common_output_structure(input_file)
                    output.update({
                        "people_count": len(enriched_data),
                        "people": enriched_data
                    })
                else:
                    enriched_data = dict(
                        sorted([self.vehicle_processor._enrich_vehicle(row) for row in collected],
                              key=lambda x: int(x[0]) if x[0].isdigit() else x[0]))
                    output = self._get_common_output_structure(input_file)
                    output.update({
                        "vehicle_count": len(enriched_data),
                        "vehicles": enriched_data
                    })
                    
                # Write output
                if not self._write_output(output_bucket, output_key, output):
                    raise ValueError(f"Failed to write output to {output_key}")
                    
                logging.info(f"Successfully processed {len(enriched_data)} {detection_type.lower()} in {input_file}")
                break
                
            except Exception as e:
                error_msg = f"ERROR in {detection_type} processing (attempt {retry_count + 1}): {str(e)}"
                logging.error(error_msg, exc_info=True)
                
                if output is None:
                    output = self._get_common_output_structure(file_key if 'input_file' not in locals() else input_file)
                
                output['processing_errors'].append(error_msg)
                
                retry_count += 1
                if retry_count <= max_retries:
                    logging.info(f"Retrying... (attempt {retry_count} of {max_retries})")
                    gc.collect()
                else:
                    self._write_output(output_bucket, output_key, output)
                    break

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        logging.info(f"Advanced {detection_type} Processing completed in {duration:.2f} seconds")


def advanced_preprocessing(detection_type,filename):
    spark = create_spark_session()
    try:
        processor = CombinedProcessor(spark, detection_type)
        processor.process_all(BUCKETS["processed"], BUCKETS["refine"], detection_type,filename)
    except Exception as e:
        logging.info(f"Fatal error in combined processing: {str(e)}")
    finally:
        if spark:
            try:
                spark.catalog.clearCache()
                spark.stop()
            except:
                pass





