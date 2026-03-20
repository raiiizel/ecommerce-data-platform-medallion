import os, sys, json
from datetime import datetime
from pyspark.sql.functions import (
    col, countDistinct, sum as spark_sum,
    round as spark_round, avg, when,
    lit, current_timestamp
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from spark_session import create_spark
from postgres_writer import write_to_postgres


def save_quality_report(report):
    os.makedirs(os.path.join(ROOT, "data/quality_reports"), exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(ROOT, f"data/quality_reports/gold_product_performance_{ts}.json")
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Quality report saved → {path}")


def transform_gold_product_performance():
    spark = create_spark()
    report = {
        "pipeline":      "gold_product_performance",
        "run_timestamp": datetime.now().isoformat(),
        "description":   "One row per product — demand, revenue, and price drift signals",
        "row_count":     0,
        "demand_tier_counts": {}
    }

    # ── 1. Read Silver ────────────────────────────────────────
    products   = spark.read.parquet(os.path.join(ROOT, "data/silver/products"))
    cart_items = spark.read.parquet(os.path.join(ROOT, "data/silver/cart_items"))

    # ── 2. Demand aggregation from cart_items ─────────────────
    demand = (
        cart_items.groupBy("product_id")
        .agg(
            spark_sum("quantity").alias("total_units_sold"),
            countDistinct("cart_id").alias("times_added_to_cart"),
            countDistinct("user_id").alias("unique_buyers"),
            spark_round(spark_sum("line_discounted_total"), 2).alias("total_revenue"),
            spark_round(avg("unit_price"), 4).alias("avg_sold_price"),
            spark_round(avg("discount_percentage"), 2).alias("avg_discount_pct")
        )
    )

    # ── 3. Join to product catalogue ──────────────────────────
    df_perf = (
        products
        .join(demand, products.id == demand.product_id, "left")
        .select(
            products.id.alias("product_id"),
            "title", "category", "brand", "sku",
            col("price").alias("catalogue_price"),
            "rating", "stock", "availability_status", "weight",
            when(col("total_units_sold").isNull(), 0)
                .otherwise(col("total_units_sold")).alias("total_units_sold"),
            when(col("times_added_to_cart").isNull(), 0)
                .otherwise(col("times_added_to_cart")).alias("times_added_to_cart"),
            when(col("unique_buyers").isNull(), 0)
                .otherwise(col("unique_buyers")).alias("unique_buyers"),
            when(col("total_revenue").isNull(), 0.0)
                .otherwise(col("total_revenue")).alias("total_revenue"),
            col("avg_sold_price"),
            col("avg_discount_pct"),
            # Price drift: avg sold price vs catalogue price
            spark_round(col("avg_sold_price") - col("price"), 4).alias("price_drift"),
            # Demand tier
            when(col("total_units_sold") == 0,  "no_sales")
            .when(col("total_units_sold") < 5,  "low")
            .when(col("total_units_sold") < 20, "medium")
            .otherwise("high")
            .alias("demand_tier"),
        )
        .withColumn("gold_processed_at", current_timestamp())
        .withColumn("source_layer", lit("silver"))
    )

    # ── 4. Write Parquet ──────────────────────────────────────
    df_perf.write.mode("overwrite").partitionBy("category").parquet(
        os.path.join(ROOT, "data/gold/product_performance")
    )

    # ── 5. Write Postgres ─────────────────────────────────────
    # Drop partition column from Postgres write — Postgres doesn't need it
    write_to_postgres(df_perf, "gold_product_performance")

    report["row_count"] = df_perf.count()
    report["demand_tier_counts"] = {
    row["demand_tier"]: row["count"]
    for row in df_perf.groupBy("demand_tier").count().collect()
}
    save_quality_report(report)
    print(f"Gold product_performance complete — {report['row_count']} rows written.")
    spark.stop()


if __name__ == "__main__":
    transform_gold_product_performance()