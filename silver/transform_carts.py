import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql.functions import (
    col, explode, trim, when, current_timestamp, lit,
    round as spark_round
)
from spark_session import create_spark
import json, os
from datetime import datetime


def log_quality_metric(report, step, df, note=""):
    report["steps"].append({
        "step": step,
        "row_count": df.count(),
        "note": note
    })


def save_quality_report(report, name):
    os.makedirs("data/quality_reports", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"data/quality_reports/{name}_{ts}.json"
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Quality report saved → {path}")


def transform_carts():
    spark = create_spark()

    report = {
        "pipeline": "carts_silver",
        "run_timestamp": datetime.now().isoformat(),
        "steps": [],
        "records_dropped": {"duplicates": 0, "failed_quality_filters": 0, "total": 0}
    }

    # ----------------------------
    # 1. Read Bronze Data
    # ----------------------------
    df_raw = (
        spark.read
        .option("multiline", "true")
        .json("data/bronze/carts/carts_raw.json")
    )

    df_carts = (
        df_raw
        .selectExpr("explode(carts) as cart")
        .select("cart.*")
    )

    log_quality_metric(report, "01_after_bronze_read", df_carts, "Raw cart count from bronze")

    # ============================================================
    # TABLE A — cart_summary (one row per cart)
    # ============================================================
    df_summary = (
        df_carts
        .select(
            col("id").cast("integer").alias("cart_id"),
            col("userId").cast("integer").alias("user_id"),
            col("totalProducts").cast("integer").alias("total_products"),
            col("totalQuantity").cast("integer").alias("total_quantity"),
            col("total").cast("double").alias("cart_total"),
            col("discountedTotal").cast("double").alias("cart_discounted_total"),
        )
        .withColumn(
            "total_savings",
            spark_round(col("cart_total") - col("cart_discounted_total"), 2)
        )
        .withColumn(
            "discount_rate",
            spark_round(
                (col("cart_total") - col("cart_discounted_total")) / col("cart_total") * 100,
                2
            )
        )
    )

    # Dedup + filter
    before = df_summary.count()
    df_summary = df_summary.dropDuplicates(["cart_id"])
    report["records_dropped"]["duplicates"] += before - df_summary.count()

    before = df_summary.count()
    df_summary = (
        df_summary
        .filter(col("cart_id").isNotNull())
        .filter(col("user_id").isNotNull())
        .filter(col("cart_total") >= 0)
        .filter(col("total_products") > 0)
    )
    report["records_dropped"]["failed_quality_filters"] += before - df_summary.count()

    df_summary = (
        df_summary
        .withColumn("silver_processed_at", current_timestamp())
        .withColumn("source_layer", lit("bronze"))
    )

    log_quality_metric(report, "02_cart_summary_ready", df_summary, "Cart-level summary table")

    df_summary.write.mode("overwrite").parquet("data/silver/cart_summary")

    # ============================================================
    # TABLE B — cart_items (one row per cart × product line item)
    # ============================================================
    df_items = (
        df_carts
        .select(
            col("id").cast("integer").alias("cart_id"),
            col("userId").cast("integer").alias("user_id"),
            explode("products").alias("item")
        )
        .select(
            "cart_id",
            "user_id",
            col("item.id").cast("integer").alias("product_id"),
            trim(col("item.title")).alias("product_title"),
            col("item.price").cast("double").alias("unit_price"),
            col("item.quantity").cast("integer").alias("quantity"),
            col("item.total").cast("double").alias("line_total"),
            col("item.discountPercentage").cast("double").alias("discount_percentage"),
            col("item.discountedTotal").cast("double").alias("line_discounted_total"),
        )
        # Recompute line_total to catch float precision issues from raw data
        .withColumn(
            "line_total_recomputed",
            spark_round(col("unit_price") * col("quantity"), 2)
        )
        .withColumn(
            "line_total_discrepancy",
            spark_round(col("line_total") - col("line_total_recomputed"), 4)
        )
    )

    # Dedup on natural composite key
    before = df_items.count()
    df_items = df_items.dropDuplicates(["cart_id", "product_id"])
    report["records_dropped"]["duplicates"] += before - df_items.count()

    before = df_items.count()
    df_items = (
        df_items
        .filter(col("cart_id").isNotNull())
        .filter(col("product_id").isNotNull())
        .filter(col("quantity") > 0)
        .filter(col("unit_price") > 0)
        .filter(col("discount_percentage").between(0, 100))
    )
    report["records_dropped"]["failed_quality_filters"] += before - df_items.count()

    df_items = (
        df_items
        .withColumn("silver_processed_at", current_timestamp())
        .withColumn("source_layer", lit("bronze"))
    )

    log_quality_metric(report, "03_cart_items_ready", df_items, "Exploded line-item table")

    # Partition by user_id — Gold joins will filter by user frequently
    df_items.write.mode("overwrite").partitionBy("user_id").parquet("data/silver/cart_items")

    # Finalize report
    report["records_dropped"]["total"] = (
        report["records_dropped"]["duplicates"] +
        report["records_dropped"]["failed_quality_filters"]
    )
    log_quality_metric(report, "04_written_to_silver", df_items, "Final line items written")
    save_quality_report(report, "carts_silver")

    print(f"Silver carts complete. {report['records_dropped']['total']} records dropped.")
    spark.stop()


if __name__ == "__main__":
    transform_carts()
