from pyspark.sql import SparkSession

def get_spark(app_name="NYC_Taxi_ETL"):
    spark = SparkSession.builder\
        .appName(app_name)\
        .config("spark.sql.session.timeZone", "UTC")\
        .getOrCreate()
    return spark