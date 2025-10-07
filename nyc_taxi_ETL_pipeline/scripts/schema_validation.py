# scripts/schema_validation.py

from pyspark.sql.functions import col, when, isnan, count
from scripts.ingestion import load_data


# -------------------------------------------------------
# 🔍 Function: validate_schema
# Purpose: Check column names and types against expectations
# -------------------------------------------------------
def validate_schema(df):
    expected_columns = {
        "VendorID": "int",
        "tpep_pickup_datetime": "timestamp",
        "tpep_dropoff_datetime": "timestamp",
        "passenger_count": "bigint",
        "trip_distance": "double",
        "fare_amount": "double"
    }

    # Compare schema column names
    actual_columns = {field.name: field.dataType.simpleString() for field in df.schema.fields}

    mismatched = {
        col_name: (expected_columns[col_name], actual_columns[col_name])
        for col_name in expected_columns
        if col_name in actual_columns and expected_columns[col_name] != actual_columns[col_name]
    }

    if mismatched:
        print("⚠️ Schema mismatches found:")
        for col_name, (expected, actual) in mismatched.items():
            print(f"   - {col_name}: expected {expected}, found {actual}")
    else:
        print("✅ Schema structure looks consistent.")

    return df


# -------------------------------------------------------
# ⚠️ Function: validate_nulls_and_invalids
# Purpose: Check for nulls or impossible values
# -------------------------------------------------------
def validate_nulls_and_invalids(df):

    null_summary = df.select(
        count(when(col("tpep_pickup_datetime").isNull(), True)).alias("pickup_nulls"),
        count(when(col("tpep_dropoff_datetime").isNull(), True)).alias("dropoff_nulls"),
        count(when(isnan("trip_distance") | col("trip_distance").isNull(), True)).alias("distance_nulls"),
        count(when(isnan("fare_amount") | col("fare_amount").isNull(), True)).alias("fare_nulls")
    )

    null_summary.show()

    invalid_rows = df.filter(
        (col("trip_distance") <= 0) | (col("fare_amount") <= 0) 
    )

    print("⚠️ Invalid record sample:")
    invalid_rows.show(5, truncate=False)

    return df


# -------------------------------------------------------
# 🏁 Main entry point
# -------------------------------------------------------

if __name__ == "__main__":
    spark, df_raw = load_data("NYC_Taxi_Schema_Validation")

    print("✅ Data loaded for validation.")
    print("Row count:", df_raw.count())

    #Run Validation checks
    df_validated = validate_schema(df_raw)
    df_validated = validate_nulls_and_invalids(df_validated)

    print("✅ Validation complete.")
    spark.stop()

