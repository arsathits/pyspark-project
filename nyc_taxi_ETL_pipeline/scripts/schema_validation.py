import sys
import os
from pyspark.sql.functions import *
from pyspark.sql.types import * 

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the get_spark function from utils/spark_session.py
from utils.spark_session import get_spark

# Get the Spark session
spark = get_spark("NYC_Taxi_Ingestion")

# Path to the data
script_dir = os.path.dirname(os.path.abspath(__file__))
path = os.path.join(script_dir, "..", "data", "yellow_tripdata_2025-07.parquet")
path = os.path.abspath(path)

# Read the parquet file
df_raw = spark.read.parquet(path)

# Check for nulls - 
df_raw.select([
    count(when(col(c).isNull(), c)).alias(c + "_nulls")
    for c in ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount"]
]).show()

# invalid timestamps (dropoff before pickup)
df_raw.filter(col('tpep_dropoff_datetime') < col('tpep_pickup_datetime'))\
    .select("tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount")\
    .show(5, truncate=False)

# zero/negative distance or fare
df_raw.filter((col("trip_distance") <= 0) | (col("fare_amount") <= 0))\
    .select("tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount")\
    .show(5, truncate=False)

# Stop the Spark session
spark.stop()


