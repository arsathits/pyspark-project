import sys
import os

# Add the parent directory (my_project) to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the get_spark function from utils/spark_session.py
from utils.spark_session import get_spark

# Get the Spark session by calling the function
spark = get_spark("NYC_Taxi_Ingestion")

# Path to the data
path = "data/yellow_tripdata_2025-07.parquet"

# Read the parquet file
df_raw = spark.read.parquet(path)

# Print row count, schema, and first 5 records
print("Row count:", df_raw.count())
df_raw.printSchema()
df_raw.show(5, truncate=False)

# Stop the Spark session
spark.stop()
