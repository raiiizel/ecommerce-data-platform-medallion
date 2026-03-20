from pyspark.sql import SparkSession
import os, sys

ROOT = os.path.dirname(os.path.abspath(__file__))

def create_spark():
    return (
        SparkSession.builder
        .appName("ecommerce-pipeline")
        .config("spark.jars", os.path.join(ROOT, "jars/postgresql-42.7.3.jar"))
        .config("spark.driver.extraClassPath", os.path.join(ROOT, "jars/postgresql-42.7.3.jar"))
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )