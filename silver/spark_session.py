from pyspark.sql import SparkSession

def create_spark():

    spark = (
        SparkSession
        .builder
        .appName("ecommerce-medallion-pipeline")
        .master("local[*]")
        .getOrCreate()
    )

    return spark