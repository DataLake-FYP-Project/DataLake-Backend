from pyspark.sql.functions import col, explode, struct, collect_list, array, lit, coalesce, size
from pyspark.sql.types import *
from .common import *
import logging

def process_animal_json_data(df):
    from pyspark.sql.functions import col, explode, struct, collect_list, size
    from pyspark.sql.types import DoubleType, ArrayType
    import logging

    # Step 0: Validate required top-level columns
    required_columns = ["frame_number", "timestamp", "detections"]
    missing_columns = [c for c in required_columns if c not in df.columns]

    if missing_columns:
        print(f"Required columns {missing_columns} not found in input DataFrame. Skipping processing.")
        return df, -1

    try:
        # Step 1: Ensure there are detections
        total_detections = df.select(size(col("detections")).alias("count")) \
                             .agg({"count": "sum"}).collect()[0][0]
        if total_detections == 0:
            logging.info("No animal detections to process. Returning original DataFrame.")
            return df, -1

        # Step 2: Frame-level columns
        frame_columns = ["frame_number", "timestamp"]

        # Step 3: Explode detections and extract only valid fields (matching JSON)
        processed = df.withColumn("detection", explode("detections")) \
            .select(
                col("frame_number"),  # frame-level
                col("timestamp"),     # frame-level
                col("detection.class_id"),
                col("detection.class_name"),
                col("detection.confidence"),
                col("detection.bbox"),
                col("detection.center"),
                col("detection.area")
            )

        # Step 4: Drop rows with any critical missing fields
        processed = processed.na.drop(subset=["class_id", "class_name", "confidence", "bbox", "area"])

        # Step 5: Clean string columns
        processed = clean_string_columns(processed)

        # ✅ No timestamp conversion anymore — it's already float

        # Step 6: Expand bbox
        processed = (
            processed.withColumn("bbox", col("bbox").cast(ArrayType(DoubleType())))
                     .withColumn("bbox_x1", col("bbox")[0])
                     .withColumn("bbox_y1", col("bbox")[1])
                     .withColumn("bbox_x2", col("bbox")[2])
                     .withColumn("bbox_y2", col("bbox")[3])
        )

        # Step 7: Expand center
        processed = (
            processed.withColumn("center_x", col("center.x"))
                     .withColumn("center_y", col("center.y"))
        )

        # Step 8: Build detection struct
        detection_fields = []
        for field in [
            "class_id", "class_name", "confidence", "bbox",
            "bbox_x1", "bbox_y1", "bbox_x2", "bbox_y2",
            "center_x", "center_y", "area"
        ]:
            if field in processed.columns:
                detection_fields.append(col(field).alias(field))

        if not detection_fields:
            logging.info("No valid detection fields found")
            return df, -1

        # Step 9: Group by frame
        reconstructed = processed.select(
            col("frame_number"),
            col("timestamp"),
            struct(*detection_fields).alias("detection")
        )

        result = (
            reconstructed.groupBy("frame_number", "timestamp")
                         .agg(collect_list("detection").alias("detections"))
                         .orderBy("frame_number")
                         .select(
                             struct(
                                 col("frame_number"),
                                 col("timestamp"),
                                 col("detections")
                             ).alias("frame_data")
                         )
        )

        return result, 1

    except Exception as e:
        print(f"Error processing animal detection data: {str(e)}")
        return df, -1
