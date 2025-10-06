import sys
import os

# Add the parent directory (my_project) to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the get_spark function from utils/spark_session.py
from utils.spark_session import get_spark

def load_data(app_name="NYC_Taxi_Ingestion"):
    # Get Spark session using centralized config
    spark = get_spark(app_name)
    
    # Dynamically locate the Parquet file in data/
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(script_dir, "..", "data", "yellow_tripdata_2025-07.parquet")
    path = os.path.abspath(path)

    # Read parquet file into DataFrame
    df_raw = spark.read.parquet(path)
    
    # Return both Spark and DF to the caller
    return spark, df_raw

if __name__ == "__main__":
    spark, df_raw = load_data()

    print("✅ Data Loaded Successfully!")
    print("Row count:", df_raw.count())
    df_raw.printSchema()
    df_raw.show(5, truncate=False)

    # Stop Spark session to release resources
    spark.stop()