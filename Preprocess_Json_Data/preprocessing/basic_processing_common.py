from pyspark.sql.functions import col, struct, collect_list, lit
from pyspark.sql.types import ArrayType, DoubleType
from Preprocess_Json_Data.preprocessing.common import clean_string_columns, handle_null_values


def process_common_json_data(df):
    default_config = {
        "default_values": {
            "tracker_id": -1,
            "class_id": -1,
            "class_name": "unknown",
            "confidence": 0.0,
            "bbox": []
        }
    }

    config = default_config | {}

    try:
        if df.count() == 0:
            print("Empty DataFrame received. Returning original.")
            return df.withColumn("processing_status", lit("no_data")), -1

        # Step 1: Clean strings and fill missing values
        processed = (
            df.transform(lambda d: handle_null_values(d, config["default_values"]))
              .transform(lambda d: clean_string_columns(d))
        )

        # Step 2: Process bbox (split into x1,y1,x2,y2)
        if "bbox" in processed.columns:
            processed = (
                processed.withColumn("bbox", col("bbox").cast(ArrayType(DoubleType())))
                         .withColumn("bbox_x1", col("bbox")[0])
                         .withColumn("bbox_y1", col("bbox")[1])
                         .withColumn("bbox_x2", col("bbox")[2])
                         .withColumn("bbox_y2", col("bbox")[3])
            )

        # Step 3: Group detections by frame
        object_fields = [
            "tracker_id", "class_id", "class_name", "confidence", "bbox",
            "bbox_x1", "bbox_y1", "bbox_x2", "bbox_y2"
        ]
        object_struct = struct(*[col(f) for f in object_fields]).alias("detection")

        grouped = (
            processed
            .select("frame_number", object_struct)
            .groupBy("frame_number")
            .agg(collect_list("detection").alias("detections"))
            .orderBy("frame_number")
        )

        # Step 4: Create final schema with frame_detections
        result_df = grouped.select(
            struct(col("frame_number"), col("detections")).alias("frame")
        ).groupBy().agg(
            collect_list("frame").alias("frame_detections")
        ).withColumn("processing_status", lit("success"))

        return result_df, 1

    except Exception as e:
        print(f"Error processing common detection data: {str(e)}")
        return df.withColumn("processing_status", lit("error")), -1
