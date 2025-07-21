import logging
from collections import defaultdict
from datetime import timezone, datetime
from pathlib import Path
import sys

from pyspark.sql import functions as F
sys.path.append(str(Path(__file__).parent.parent))
from ..connectors.minio_connector import MinIOConnector
import pyspark.sql.types as T


class ParkingProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.minio = MinIOConnector(spark)

    def _process_parking_format(self, df):
        if "frame_detections" not in df.columns:
            logging.warning("Missing 'frame_detections'. Skipping.")
            return None

        # Extract frames
        df = df.withColumn("frame", F.explode("frame_detections"))
        df = df.select("*", F.col("frame.*")).drop("frame", "frame_detections")

        # Flatten slots from struct to map
        df = df.withColumn("slots_json", F.to_json("slots"))
        df = df.withColumn("slots_map", F.from_json(F.col("slots_json"),
            T.MapType(T.StringType(), T.StructType([
                T.StructField("occupied", T.BooleanType()),
                T.StructField("bbox", T.ArrayType(T.DoubleType()))
            ]))
        )).drop("slots", "slots_json").withColumnRenamed("slots_map", "slots")

        # Explode each slot's state per frame
        df = df.withColumn("slot", F.explode(F.map_entries("slots")))
        df = df.select(
            "timestamp_sec",
            F.col("slot.key").alias("slot_id"),
            F.col("slot.value.occupied").alias("occupied")
        ).withColumn("slot_id", F.col("slot_id").cast("string")).orderBy("slot_id", "timestamp_sec")

        return df

    def _analyze_slot_transitions(self, df):
        slot_ids = [row["slot_id"] for row in df.select("slot_id").distinct().collect()]
        results = []
        final_occupancy = {}
        free_count = 0

        for sid in slot_ids:
            sdf = df.filter(F.col("slot_id") == sid).orderBy("timestamp_sec")
            rows = sdf.collect()

            became_free = 0
            became_occupied = 0
            last_state = None
            last_change_time = None
            total_free = 0.0
            total_occupied = 0.0
            parking_sessions = []

            for row in rows:
                t = row["timestamp_sec"]
                occ = row["occupied"]

                if last_state is not None and occ != last_state:
                    if last_state:  # occupied → free
                        became_free += 1
                        duration = t - last_change_time
                        total_occupied += duration
                        parking_sessions.append({
                            "entry_time": round(last_change_time, 3),
                            "exit_time": round(t, 3),
                            "duration": round(duration, 3)
                        })
                    else:  # free → occupied
                        became_occupied += 1
                        duration = t - last_change_time
                        total_free += duration
                    last_change_time = t
                elif last_state is None:
                    last_change_time = t

                last_state = occ

            # Handle duration from last state till the last timestamp
            if last_state is not None and last_change_time is not None and rows:
                last_row_time = max(row["timestamp_sec"] for row in rows)
                duration = last_row_time - last_change_time
                if last_state:
                    total_occupied += duration
                else:
                    total_free += duration

            total_time = total_occupied + total_free
            free_percentage = total_free / total_time if total_time > 0 else 0.0

            # Get the actual final status based on the latest timestamp
            latest_row = max(rows, key=lambda r: r["timestamp_sec"]) if rows else None
            final_state = latest_row["occupied"] if latest_row else False

            results.append({
                "slot_id": int(sid),
                "slot_status": "free" if not final_state else "occupied",
                "state_transitions": {
                    "became_free": became_free,
                    "became_occupied": became_occupied
                },
                "time_metrics": {
                    "total_occupied_seconds": round(total_occupied, 3),
                    "total_free_seconds": round(total_free, 3),
                    "free_percentage": round(free_percentage, 3)
                },
                "parking_sessions": parking_sessions
            })

            final_occupancy[int(sid)] = final_state
            if not final_state:
              free_count += 1 

        return results, final_occupancy, free_count
