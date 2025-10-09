import os
from pyspark.sql.functions import *
from scripts.ingestion import load_data

def apply_basic_cleaning(df):
    """
    Apply strict cleaning rules (drop impossible rows) and add a borderline_flag.
    Returns: cleaned_df
    """
    # Define retain condition (what we keep)    
    retain_cond = (
        (col("fare_amount") >= 0) &
        (col("trip_distance") >= 0) &
        (col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime")) &
        ~((col("trip_distance") == 0) & (col("fare_amount") == 0))
    )


    cleaned_df = df.filter(retain_cond)

    cleaned_df = cleaned_df.withColumn(
        "borderline_flag",
        when((col("trip_distance") == 0) & (col("fare_amount") > 0), lit("MIN_FARE_CASE"))
        .otherwise(lit("Normal"))
    )

    return cleaned_df


if __name__ == "__main__":
    # Load data via reusable ingestion function (keeps Spark/session handling consistent)
    spark, df_raw = load_data("NYC_Taxi_Data_Cleaning")

    print("✅ Loaded raw data for cleaning")
    raw_count = df_raw.count()
    print("Raw row count:", raw_count)

    # 1) Apply cleaning rules
    cleaned_df = apply_basic_cleaning(df_raw)

    # 2) Basic stats / verification
    clean_count = cleaned_df.count()
    dropped = raw_count - clean_count
    print(f"Clean row count: {clean_count}  (dropped {dropped} rows)")

    print("\nBorderline flag distribution:")
    cleaned_df.groupBy("borderline_flag").count().show()

    # 3) Show sample of records that were dropped (invalids)
    retain_cond = (
        (col("fare_amount") >= 0) &
        (col("trip_distance") >= 0) &
        (col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime")) &
        ~((col("trip_distance") == 0) & (col("fare_amount") == 0))
    )

    invalid_rows = df_raw.filter(~retain_cond)
    print("\nSample invalid rows (dropped):")
    invalid_rows.show(5, truncate=False)

    # 4) Persist cleaned data (so downstream steps read the snapshot)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.abspath(os.path.join(script_dir, "..", "data", "cleaned", "yellow_tripdata_2025_07_cleaned"))
    os.makedirs(out_dir, exist_ok=True)

    print(f"\nWriting cleaned data to: {out_dir} (parquet, overwrite)")
    cleaned_df.write.mode("overwrite").parquet(out_dir)

    # stop spark
    spark.stop()
    print("✅ Cleaning finished and Spark stopped.")