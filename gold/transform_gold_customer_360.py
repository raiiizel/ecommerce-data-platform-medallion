import os, sys, json
from datetime import datetime
from pyspark.sql.functions import (
    col, countDistinct, sum as spark_sum,
    round as spark_round, avg, max as spark_max,
    when, lit, current_timestamp
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from spark_session import create_spark
from postgres_writer import write_to_postgres


def save_quality_report(report):
    os.makedirs(os.path.join(ROOT, "data/quality_reports"), exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(ROOT, f"data/quality_reports/gold_customer_360_{ts}.json")
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Quality report saved → {path}")


def transform_gold_customer_360():
    spark = create_spark()
    report = {
        "pipeline":      "gold_customer_360",
        "run_timestamp": datetime.now().isoformat(),
        "description":   "One row per user — spend, behaviour, and favourite category",
        "row_count":     0
    }

    # ── 1. Read Silver ────────────────────────────────────────
    users = (
        spark.read.parquet(os.path.join(ROOT, "data/silver/users"))
        .select(
            "id", "first_name", "last_name", "email", "gender",
            "age_derived", "address_city", "address_country",
            "company_department", "role"
        )
    )
    cart_items = spark.read.parquet(os.path.join(ROOT, "data/silver/cart_items"))
    products   = (
        spark.read.parquet(os.path.join(ROOT, "data/silver/products"))
        .select(col("id").alias("product_id"), "category")
    )

    # ── 2. Cart-level spend per user ──────────────────────────
    # Rename user_id immediately to avoid ambiguity in later joins
    user_cart_agg = (
        cart_items
        .groupBy("user_id", "cart_id")
        .agg(
            spark_sum("quantity").alias("cart_quantity"),
            spark_sum("line_discounted_total").alias("cart_spend")
        )
        .groupBy("user_id")
        .agg(
            countDistinct("cart_id").alias("total_carts"),
            spark_sum("cart_quantity").alias("total_items_bought"),
            spark_round(spark_sum("cart_spend"), 2).alias("total_spend"),
            spark_round(avg("cart_spend"), 2).alias("avg_cart_value"),
            spark_round(avg("cart_quantity"), 1).alias("avg_basket_size")
        )
        .withColumnRenamed("user_id", "cart_user_id")
    )

    # ── 3. Favourite category per user ────────────────────────
    category_qty = (
        cart_items
        .join(products, on="product_id", how="left")
        .groupBy("user_id", "category")
        .agg(spark_sum("quantity").alias("qty_in_category"))
    )
    user_max_qty = (
        category_qty
        .groupBy("user_id")
        .agg(spark_max("qty_in_category").alias("max_qty"))
        .withColumnRenamed("user_id", "max_user_id")
    )
    favourite_category = (
        category_qty
        .join(user_max_qty, category_qty.user_id == user_max_qty.max_user_id)
        .filter(col("qty_in_category") == col("max_qty"))
        .select(
            category_qty.user_id.alias("fav_user_id"),
            col("category").alias("favourite_category")
        )
        .dropDuplicates(["fav_user_id"])
    )

    # ── 4. Discount behaviour ─────────────────────────────────
    discount_agg = (
        cart_items
        .groupBy("user_id")
        .agg(
            spark_round(avg("discount_percentage"), 2).alias("avg_discount_captured_pct"),
            spark_round(
                spark_sum(col("line_total") - col("line_discounted_total")), 2
            ).alias("total_savings")
        )
        .withColumnRenamed("user_id", "disc_user_id")
    )

    # ── 5. Join onto user spine ───────────────────────────────
    # All joined DataFrames have renamed user_id columns so no ambiguity
    df_360 = (
        users
        .join(user_cart_agg,      users.id == user_cart_agg.cart_user_id,      "left")
        .join(favourite_category, users.id == favourite_category.fav_user_id,   "left")
        .join(discount_agg,       users.id == discount_agg.disc_user_id,        "left")
        .select(
            users.id.alias("user_id"),
            "first_name", "last_name", "email", "gender",
            col("age_derived").alias("age"),
            "address_city", "address_country", "company_department", "role",
            "total_carts", "total_items_bought", "total_spend",
            "avg_cart_value", "avg_basket_size", "favourite_category",
            "avg_discount_captured_pct", "total_savings",
        )
        .withColumn("total_carts",
            when(col("total_carts").isNull(), 0).otherwise(col("total_carts")))
        .withColumn("total_items_bought",
            when(col("total_items_bought").isNull(), 0).otherwise(col("total_items_bought")))
        .withColumn("total_spend",
            when(col("total_spend").isNull(), 0.0).otherwise(col("total_spend")))
        .withColumn("avg_cart_value",
            when(col("avg_cart_value").isNull(), 0.0).otherwise(col("avg_cart_value")))
        .withColumn("avg_basket_size",
            when(col("avg_basket_size").isNull(), 0.0).otherwise(col("avg_basket_size")))
        .withColumn("total_savings",
            when(col("total_savings").isNull(), 0.0).otherwise(col("total_savings")))
        .withColumn("gold_processed_at", current_timestamp())
        .withColumn("source_layer", lit("silver"))
    )

    # ── 6. Write Parquet ──────────────────────────────────────
    df_360.write.mode("overwrite").parquet(
        os.path.join(ROOT, "data/gold/customer_360")
    )

    # ── 7. Write Postgres ─────────────────────────────────────
    write_to_postgres(df_360, "gold_customer_360")

    report["row_count"] = df_360.count()
    save_quality_report(report)
    print(f"Gold customer_360 complete — {report['row_count']} rows written.")
    spark.stop()


if __name__ == "__main__":
    transform_gold_customer_360()