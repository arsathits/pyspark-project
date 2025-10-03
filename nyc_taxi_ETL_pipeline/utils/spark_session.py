import warnings
from pyspark.sql import SparkSession

def get_spark(app_name="NYC_Taxi_ETL"):
    """
    Creates and returns a SparkSession with reduced logging
    """
    # Suppress Python warnings
    warnings.filterwarnings('ignore')
    
    # Create Spark session
    spark = SparkSession.builder\
        .appName(app_name)\
        .config("spark.sql.session.timeZone", "UTC")\
        .getOrCreate()
    
    # Set Spark log level to ERROR (reduces console output)
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark