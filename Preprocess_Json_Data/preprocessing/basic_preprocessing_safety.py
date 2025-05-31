from pyspark.sql.functions import col, explode, struct, collect_list, array, lit, coalesce, size
from pyspark.sql.types import *
from Preprocess_Json_Data.preprocessing.common import clean_string_columns, handle_null_values


def process_safety_json_data(df):
    required_columns = ["frame_number", "people"]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        print(f"Required columns {missing_columns} not found in input DataFrame. Skipping processing.")
        return df, -1

    default_config = {
        "default_values": {
            "hardhat": False,
            "mask": False,
            "safety_vest": False,
            "tracker_id": -1,
            "safety_status": "Unknown",
            "missing_items": [],
            "bbox": []
        }
    }
    config = default_config | {}

    try:
        total_people = df.select(size(col("people")).alias("count")).agg({"count": "sum"}).collect()[0][0]
        if total_people == 0:
            print("No people detections found in any frame. Returning original DataFrame.")
            return df, -1

        # Step 1: Explode people
        frame_columns = ["frame_number"]
        processed = df.withColumn("person", explode("people")) \
                      .select(
                          *[col(c) for c in frame_columns],
                          col("person.*")
                      )

        # Apply cleaning and default filling
        processed = (
            processed
            .transform(lambda df: handle_null_values(df, config["default_values"]))
            .transform(lambda df: clean_string_columns(df))
        )

        # Handle bbox if present
        if "bbox" in processed.columns:
            processed = (
                processed.withColumn("bbox", col("bbox").cast(ArrayType(DoubleType())))
                         .withColumn("bbox_x1", col("bbox")[0])
                         .withColumn("bbox_y1", col("bbox")[1])
                         .withColumn("bbox_x2", col("bbox")[2])
                         .withColumn("bbox_y2", col("bbox")[3])
            )

        # Step 2: Reconstruct people struct
        person_fields = []
        possible_fields = [
            ("hardhat", "hardhat"),
            ("mask", "mask"),
            ("safety_vest", "safety_vest"),
            ("tracker_id", "tracker_id"),
            ("safety_status", "safety_status"),
            ("missing_items", "missing_items"),
            ("bbox", "bbox"),
            ("bbox_x1", "bbox_x1"),
            ("bbox_y1", "bbox_y1"),
            ("bbox_x2", "bbox_x2"),
            ("bbox_y2", "bbox_y2")
        ]

        for field_name, col_name in possible_fields:
            if col_name in processed.columns:
                person_fields.append(col(col_name).alias(field_name))

        reconstructed_people = processed.select(
            *[col(c) for c in frame_columns],
            struct(*person_fields).alias("person")
        )

        # Step 3: Regroup by frame_number
        result = (
            reconstructed_people
            .groupBy(*frame_columns)
            .agg(collect_list("person").alias("people"))
            .orderBy("frame_number")
            .select(
                struct(
                    *[col(c) for c in frame_columns],
                    col("people")
                ).alias("frame_data")
            )
        )

        return result, 1

    except Exception as e:
        print(f"Error processing safety gear data: {str(e)}")
        return df, -1