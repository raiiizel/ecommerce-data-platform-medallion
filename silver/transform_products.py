from pyspark.sql.functions import (
    explode, col, when, current_timestamp, trim, lower, lit, count
)
from pyspark.sql import DataFrame
from spark_session import create_spark
import json
import os
from datetime import datetime


def log_quality_metric(report: dict, step: str, df: DataFrame, note: str = ""):
    """Snapshot row count at each pipeline stage."""
    report["steps"].append({
        "step": step,
        "row_count": df.count(),
        "note": note
    })


def compute_null_summary(df: DataFrame) -> dict:
    """Return null counts per column as a plain dict."""
    null_counts = {}
    for field in df.schema.fields:
        n = df.filter(col(field.name).isNull()).count()
        if n > 0:
            null_counts[field.name] = n
    return null_counts


def save_quality_report(report: dict):
    """Persist the quality report as a timestamped JSON file."""
    os.makedirs("data/quality_reports", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"data/quality_reports/products_silver_{timestamp}.json"
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Data quality report saved → {path}")


def transform_products():
    spark = create_spark()

    # Initialize quality report
    report = {
        "pipeline": "products_silver",
        "run_timestamp": datetime.now().isoformat(),
        "steps": [],
        "null_summary_after_cleaning": {},
        "records_dropped": {}
    }

    # ----------------------------
    # 1. Read Bronze Data
    # ----------------------------
    df_raw = (
        spark.read
        .option("multiline", "true")
        .json("../data/bronze/products/products_raw.json")
    )

    # ----------------------------
    # 2. Flatten Nested Structure
    # ----------------------------
    df_products = (
        df_raw
        .select(explode("products").alias("product"))
        .select("product.*")
    )

    # ----------------------------
    # 3. Select Relevant Columns
    # ----------------------------
    df_products = df_products.select(
        "id", "title", "description", "category",
        "price", "discountPercentage", "rating",
        "stock", "brand", "availabilityStatus", "sku", "weight"
    )

    log_quality_metric(report, "01_after_bronze_read", df_products, "Raw record count from bronze")

    # ----------------------------
    # 4. Standardize Column Names
    # ----------------------------
    df_products = (
        df_products
        .withColumnRenamed("discountPercentage", "discount_percentage")
        .withColumnRenamed("availabilityStatus", "availability_status")
    )

    # ----------------------------
    # 5. Enforce Data Types
    # ----------------------------
    df_products = (
        df_products
        .withColumn("id",                  col("id").cast("integer"))
        .withColumn("price",               col("price").cast("double"))
        .withColumn("discount_percentage", col("discount_percentage").cast("double"))
        .withColumn("rating",              col("rating").cast("double"))
        .withColumn("stock",               col("stock").cast("integer"))
        .withColumn("weight",              col("weight").cast("double"))
    )

    # ----------------------------
    # 6. Normalize String Fields
    # ----------------------------
    df_products = (
        df_products
        .withColumn("title",               trim(col("title")))
        .withColumn("brand",               trim(col("brand")))
        .withColumn("category",            lower(trim(col("category"))))
        .withColumn("availability_status", lower(trim(col("availability_status"))))
        .withColumn("sku",                 trim(col("sku")))
    )

    # ----------------------------
    # 7. Handle Missing Values
    # ----------------------------
    df_products = (
        df_products
        .withColumn("brand",               when(col("brand").isNull() | (col("brand") == ""), "unknown").otherwise(col("brand")))
        .withColumn("rating",              when(col("rating").isNull(), 0.0).otherwise(col("rating")))
        .withColumn("stock",               when(col("stock").isNull(), 0).otherwise(col("stock")))
        .withColumn("discount_percentage", when(col("discount_percentage").isNull(), 0.0).otherwise(col("discount_percentage")))
        .withColumn("weight",              when(col("weight").isNull(), 0.0).otherwise(col("weight")))
    )

    log_quality_metric(report, "02_after_cleaning", df_products, "After nulls handled and types enforced")

    # Capture null summary on key columns after cleaning
    report["null_summary_after_cleaning"] = compute_null_summary(
        df_products.select("id", "price", "rating", "stock", "category", "brand")
    )

    # ----------------------------
    # 8. Deduplicate Records
    # ----------------------------
    count_before_dedup = df_products.count()
    df_products = df_products.dropDuplicates(["id"])
    count_after_dedup = df_products.count()

    report["records_dropped"]["duplicates"] = count_before_dedup - count_after_dedup
    log_quality_metric(report, "03_after_dedup", df_products, "After removing duplicate product IDs")

    # ----------------------------
    # 9. Data Quality Filters
    # ----------------------------
    count_before_filters = df_products.count()

    df_products = (
        df_products
        .filter(col("id").isNotNull())
        .filter(col("price") > 0)
        .filter(col("rating").between(0, 5))
        .filter(col("stock") >= 0)
        .filter(col("discount_percentage").between(0, 100))
    )

    count_after_filters = df_products.count()
    report["records_dropped"]["failed_quality_filters"] = count_before_filters - count_after_filters
    log_quality_metric(report, "04_after_quality_filters", df_products, "After business rule filters")

    # Total dropped across the whole pipeline
    report["records_dropped"]["total"] = (
        report["records_dropped"]["duplicates"] +
        report["records_dropped"]["failed_quality_filters"]
    )

    # ----------------------------
    # 10. Add Metadata Columns
    # ----------------------------
    df_products = (
        df_products
        .withColumn("silver_processed_at", current_timestamp())
        .withColumn("source_layer", lit("bronze"))
    )

    # ----------------------------
    # 11. Write Silver Dataset
    # ----------------------------
    (
        df_products
        .write
        .mode("overwrite")
        .partitionBy("category")
        .parquet("../data/silver/products")
    )

    log_quality_metric(report, "05_written_to_silver", df_products, "Final record count written to silver")

    # ----------------------------
    # 12. Save Quality Report
    # ----------------------------
    save_quality_report(report)

    print(f"Silver products pipeline complete. {report['records_dropped']['total']} records dropped total.")
    spark.stop()


if __name__ == "__main__":
    transform_products()