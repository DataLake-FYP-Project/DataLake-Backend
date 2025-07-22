from collections import Counter, defaultdict
from datetime import datetime
import statistics
from pyspark.sql.functions import col, explode, count, min as spark_min, max as spark_max, collect_list, avg, first, expr
import pyspark.sql.functions as F


class SchoolProcessor:
    def __init__(self, spark):
        self.spark = spark

    def format_processed_data(self, df):
        """Extract and flatten event detections from frames"""
        df = df.select(
            col("frame_number"),
            col("timestamp").alias("frame_timestamp"),
            explode("detections").alias("event")
        )

        df = df.select(
            "frame_number",
            "frame_timestamp",
            col("event.event_id").alias("event_id"),
            col("event.event_type").alias("event_type"),
            col("event.timestamp").cast("timestamp").alias("event_timestamp"),
            col("event.location").alias("location"),
            col("event.confidence").alias("confidence"),
            col("event.involved_person_id").alias("person_id"),
            col("event.duration_seconds").alias("duration_seconds"),
            col("event.notes").alias("notes"),
            col("event.alert_level").alias("alert_level"),
            col("event.response_required").alias("response_required"),
            col("event.multiple_persons_involved").alias("multiple_involved"),
            col("event.person_roles").alias("person_roles")
        ).filter(col("event_id").isNotNull())

        return df

    def _group_data(self, df):
        """Group and aggregate data by event_id"""
        return df.groupBy("event_id").agg(
            spark_min("event_timestamp").alias("start_time"),
            spark_max("event_timestamp").alias("end_time"),
            count("*").alias("frame_occurrences"),
            avg("confidence").alias("avg_confidence"),
            first("event_type", ignorenulls=True).alias("event_type"),
            first("location", ignorenulls=True).alias("location"),
            first("notes", ignorenulls=True).alias("notes"),
            first("response_required", ignorenulls=True).alias("response_required"),
            F.max("multiple_involved").alias("multiple_persons_involved"),
            collect_list("person_id").alias("persons_involved"),
            collect_list("alert_level").alias("alert_levels"),
            collect_list("duration_seconds").alias("durations"),
            F.flatten(collect_list("person_roles")).alias("all_roles")
        )

    def _enrich_data(self, row):
        """Create enriched dict for an event"""
        roles = row["all_roles"] or []
        role_counts = Counter(roles)
        most_common_role = role_counts.most_common(1)[0][0] if roles else "Unknown"

        alert_counts = Counter(row["alert_levels"] or [])
        most_common_alert = alert_counts.most_common(1)[0][0] if alert_counts else "Unknown"

        durations = row["durations"] or []
        avg_duration = statistics.mean(durations) if durations else 0.0
        duration_std = statistics.stdev(durations) if len(durations) > 1 else 0.0

        return row["event_id"], {
            "event_type": row["event_type"],
            "location": row["location"],
            "notes": row["notes"],
            "start_time": row["start_time"].isoformat(),
            "end_time": row["end_time"].isoformat(),
            "duration_seconds": (row["end_time"] - row["start_time"]).total_seconds(),
            "frame_occurrences": row["frame_occurrences"],
            "avg_confidence": row["avg_confidence"] or 0.0,
            "avg_event_duration": avg_duration,
            "event_duration_stddev": duration_std,
            "response_required": row["response_required"],
            "most_common_alert_level": most_common_alert,
            "alert_level_distribution": dict(alert_counts),
            "involved_persons": list(set(row["persons_involved"] or [])),
            "unique_roles": list(set(roles)),
            "most_common_role": most_common_role,
            "multiple_persons_involved": row["multiple_persons_involved"]
        }
