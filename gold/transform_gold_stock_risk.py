import os, sys, json
from datetime import datetime
from pyspark.sql.functions import (
    col, sum as spark_sum, when,
    lit, current_timestamp
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from spark_session import create_spark
from postgres_writer import write_to_postgres

CRITICAL_THRESHOLD = 0
HIGH_THRESHOLD     = 5
MEDIUM_THRESHOLD   = 20


def save_quality_report(report):
    os.makedirs(os.path.join(ROOT, "data/quality_reports"), exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(ROOT, f"data/quality_reports/gold_stock_risk_{ts}.json")
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Quality report saved → {path}")


def transform_gold_stock_risk():
    spark = create_spark()
    report = {
        "pipeline":      "gold_stock_risk",
        "run_timestamp": datetime.now().isoformat(),
        "description":   "Operational table — products where demand threatens stock levels",
        "thresholds":    {
            "critical": CRITICAL_THRESHOLD,
            "high":     HIGH_THRESHOLD,
            "medium":   MEDIUM_THRESHOLD
        },
        "row_count":          0,
        "risk_level_counts":  {}
    }

    # ── 1. Read Silver ────────────────────────────────────────
    products = (
        spark.read.parquet(os.path.join(ROOT, "data/silver/products"))
        .select("id", "title", "category", "brand", "sku", "stock", "availability_status")
    )
    cart_items = spark.read.parquet(os.path.join(ROOT, "data/silver/cart_items"))

    # ── 2. Total pending demand per product ───────────────────
    pending_demand = (
        cart_items.groupBy("product_id")
        .agg(spark_sum("quantity").alias("pending_demand"))
    )

    # ── 3. Join and compute net stock + risk level ────────────
    df_risk = (
        products
        .join(pending_demand, products.id == pending_demand.product_id, "left")
        .withColumn("pending_demand",
            when(col("pending_demand").isNull(), 0).otherwise(col("pending_demand")))
        .withColumn("net_stock", col("stock") - col("pending_demand"))
        .withColumn("risk_level",
            when(col("net_stock") <= CRITICAL_THRESHOLD, "critical")
            .when(col("net_stock") <= HIGH_THRESHOLD,    "high")
            .when(col("net_stock") <= MEDIUM_THRESHOLD,  "medium")
            .otherwise("healthy"))
        .withColumn("stock_coverage_ratio",
            when(col("pending_demand") > 0,
                col("stock") / col("pending_demand")
            ).otherwise(None))
        .select(
            products.id.alias("product_id"),
            "title", "category", "brand", "sku",
            "availability_status",
            col("stock").alias("current_stock"),
            "pending_demand", "net_stock",
            "stock_coverage_ratio", "risk_level",
        )
        .withColumn("gold_processed_at", current_timestamp())
        .withColumn("source_layer", lit("silver"))
    )

    # ── 4. Write Parquet (partitioned by risk_level) ──────────
    df_risk.write.mode("overwrite").partitionBy("risk_level").parquet(
        os.path.join(ROOT, "data/gold/stock_risk")
    )

    # ── 5. Write Postgres ─────────────────────────────────────
    write_to_postgres(df_risk, "gold_stock_risk")

    report["row_count"] = df_risk.count()
    report["risk_level_counts"] = {
    row["risk_level"]: row["count"]
    for row in df_risk.groupBy("risk_level").count().collect()
}
    save_quality_report(report)

    critical = report["risk_level_counts"].get("critical", 0)
    high     = report["risk_level_counts"].get("high", 0)
    print(f"Gold stock_risk complete — {report['row_count']} products evaluated.")
    print(f"  critical: {critical}  |  high: {high}")
    spark.stop()


if __name__ == "__main__":
    transform_gold_stock_risk()