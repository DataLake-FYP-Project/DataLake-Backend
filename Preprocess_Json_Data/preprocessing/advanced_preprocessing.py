import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from Preprocess_Json_Data.preprocessing.advanced_preprocessing_animal import AnimalProcessor
from Preprocess_Json_Data.preprocessing.advanced_preprocessing_safety import SafetyProcessor
from .advanced_preprocessing_common import CommonProcessor

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
        self.safety_processor = SafetyProcessor(spark)
        self.animal_processor = AnimalProcessor(spark)
        self.common_processor = CommonProcessor(spark)

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
            elif processor.__class__.__name__ == "SafetyProcessor":
                processed_df = processor._process_safety_format(df)
            elif processor.__class__.__name__ == "VehicleProcessor":  
                processed_df = processor._process_vehicle_format(df)
            elif processor.__class__.__name__ == "AnimalProcessor":  
                processed_df = processor._process_frame_detections_format(df)

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
        
        elif detection_type == "Safety":
            logging.info("Processing Safety Detections")
            try:
                # 1. Load input JSON file from MinIO
                safety_file = self.safety_processor.minio.get_json_file(
                    input_bucket,
                    f"safety_detection/preprocessed_{filename}"
                )

                # 2. Preprocess file
                processed_df = self._process_file(
                    self.safety_processor,
                    input_bucket,
                    output_bucket,
                    safety_file,
                    "safety_detection"
                )

                if processed_df == "all_invalid":
                    logging.info("Stopping preprocessing - all safety tracker IDs are -1")
                    return -1

                if processed_df is None:
                    logging.info("No valid safety detections to process")
                    return -1

                # 3. Group and enrich data
                grouped = self.safety_processor._group_data(processed_df)
                collected = grouped.collect()

                # 4. Enrich each group using updated logic
                enriched_data = dict(
                    sorted(
                        [self.safety_processor._enrich_safety(row) for row in collected],
                        key=lambda x: int(x[0])
                    )
                )

                # 5. Build common output structure
                output = self._get_common_output_structure(filename)
                output.update({
                    "safety_count": len(enriched_data),
                    "safety_objects": enriched_data
                })

                # 6. Save enriched output to MinIO
                out_path = f"safety_detection/refine_{filename}"
                self._write_output(output_bucket, out_path, output)

                logging.info(f"Successfully processed {len(enriched_data)} safety objects in {safety_file}")

            except Exception as e:
                logging.info(f"ERROR in safety processing: {str(e)}")
                return -1

        elif detection_type == "Animal":
            logging.info("Processing Animal Detections")
            try:
                animal_file = self.animal_processor.minio.get_json_file(input_bucket, f"animal_detection/preprocessed_{filename}")
                processed_df = self._process_file(self.animal_processor, input_bucket, output_bucket, animal_file, "animal_detection")
                
                if processed_df == "all_invalid":
                    logging.info("Stopping preprocessing - all animal tracker IDs are -1")
                    return -1
                    
                if processed_df is None:
                    logging.info("No valid animal detections to process")
                    return -1
                    
                grouped = self.animal_processor._group_data(processed_df)
                collected = grouped.collect()
                enriched_data = dict(
                    sorted([self.animal_processor._enrich_animal(row) for row in collected],
                        key=lambda x: int(x[0])))
                
                output = self._get_common_output_structure(animal_file)
                output.update({
                    "animal_count": len(enriched_data),
                    "animals": enriched_data
                })
                out_path = f"animal_detection/refine_{filename}"
                self._write_output(output_bucket, out_path, output)
                logging.info(f"Successfully processed {len(enriched_data)} animals in {animal_file}")
                
            except Exception as e:
                logging.info(f"ERROR in animal processing: {str(e)}")
                return -1

        elif detection_type == "Common":
            logging.info("Processing Common Detections")
            try:
                common_file = self.common_processor.minio.get_json_file(input_bucket,
                                                                        f"common_detection/preprocessed_{filename}")
                processed_df = self._process_file(self.common_processor, input_bucket, output_bucket, common_file,
                                                  "common_detection")

                if processed_df == "all_invalid":
                    logging.info("Stopping preprocessing - all common data tracker IDs are -1")
                    return -1

                if processed_df is None:
                    logging.info("No valid common data detections to process")
                    return -1

                grouped = self.common_processor._group_data(processed_df)
                collected = grouped.collect()
                enriched_data = dict(
                    sorted([self.common_processor._enrich_common(row) for row in collected],
                           key=lambda x: int(x[0])))

                output = self._get_common_output_structure(common_file)
                output.update({
                    "common_count": len(enriched_data),
                    "common": enriched_data
                })
                out_path = f"common_detection/refine_{filename}"
                self._write_output(output_bucket, out_path, output)
                logging.info(f"Successfully processed {len(enriched_data)} animals in {common_file}")

            except Exception as e:
                logging.info(f"ERROR in common data processing: {str(e)}")
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
