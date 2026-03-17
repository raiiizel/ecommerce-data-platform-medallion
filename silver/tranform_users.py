from pyspark.sql.functions import (
    col, trim, lower, when, current_timestamp, lit,
    to_date, floor, months_between, current_date
)
from pyspark.sql import DataFrame
from spark_session import create_spark
import json, os
from datetime import datetime


def log_quality_metric(report, step, df, note=""):
    report["steps"].append({
        "step": step,
        "row_count": df.count(),
        "note": note
    })


def compute_null_summary(df):
    return {
        field.name: df.filter(col(field.name).isNull()).count()
        for field in df.schema.fields
        if df.filter(col(field.name).isNull()).count() > 0
    }


def save_quality_report(report, name):
    os.makedirs("data/quality_reports", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"data/quality_reports/{name}_{ts}.json"
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Quality report saved → {path}")


def transform_users():
    spark = create_spark()

    report = {
        "pipeline": "users_silver",
        "run_timestamp": datetime.now().isoformat(),
        "steps": [],
        "null_summary_after_cleaning": {},
        "records_dropped": {"duplicates": 0, "failed_quality_filters": 0, "total": 0},
        "pii_fields_removed": [
            "password", "ssn", "ein", "ip", "macAddress",
            "userAgent", "bank.cardNumber", "bank.iban",
            "crypto", "image"
        ]
    }

    # ----------------------------
    # 1. Read Bronze Data
    # ----------------------------
    df_raw = (
        spark.read
        .option("multiline", "true")
        .json("../data/bronze/users/users_raw.json")
    )

    df_users = (
        df_raw
        .select("users")
        .selectExpr("explode(users) as user")
        .select("user.*")
    )

    log_quality_metric(report, "01_after_bronze_read", df_users, "Raw record count from bronze")

    # ----------------------------
    # 2. Flatten Nested Structs
    # ----------------------------
    df_users = (
        df_users
        # Personal
        .withColumn("hair_color",          col("hair.color"))
        .withColumn("hair_type",           col("hair.type"))

        # Address
        .withColumn("address_street",      col("address.address"))
        .withColumn("address_city",        col("address.city"))
        .withColumn("address_state",       col("address.state"))
        .withColumn("address_state_code",  col("address.stateCode"))
        .withColumn("address_postal_code", col("address.postalCode"))
        .withColumn("address_country",     col("address.country"))
        .withColumn("address_lat",         col("address.coordinates.lat").cast("double"))
        .withColumn("address_lng",         col("address.coordinates.lng").cast("double"))

        # Company
        .withColumn("company_name",        col("company.name"))
        .withColumn("company_department",  col("company.department"))
        .withColumn("company_title",       col("company.title"))

        # Bank (non-sensitive only)
        .withColumn("bank_card_type",      col("bank.cardType"))
        .withColumn("bank_currency",       col("bank.currency"))
        .withColumn("bank_card_expire",    col("bank.cardExpire"))
    )

    # ----------------------------
    # 3. Select Columns — PII Dropped Here
    # ----------------------------
    df_users = df_users.select(
        "id", "firstName", "lastName", "age", "gender",
        "email", "phone", "username", "birthDate",
        "bloodGroup", "height", "weight", "eyeColor",
        "hair_color", "hair_type",
        "address_street", "address_city", "address_state",
        "address_state_code", "address_postal_code", "address_country",
        "address_lat", "address_lng",
        "university", "role",
        "company_name", "company_department", "company_title",
        "bank_card_type", "bank_currency", "bank_card_expire"
        # password, ssn, ein, ip, macAddress, userAgent,
        # cardNumber, iban, crypto → intentionally excluded (PII / sensitive)
    )

    # ----------------------------
    # 4. Standardize Column Names
    # ----------------------------
    df_users = (
        df_users
        .withColumnRenamed("firstName",  "first_name")
        .withColumnRenamed("lastName",   "last_name")
        .withColumnRenamed("birthDate",  "birth_date")
        .withColumnRenamed("bloodGroup", "blood_group")
        .withColumnRenamed("eyeColor",   "eye_color")
    )

    # ----------------------------
    # 5. Enforce Data Types
    # ----------------------------
    df_users = (
        df_users
        .withColumn("id",         col("id").cast("integer"))
        .withColumn("age",        col("age").cast("integer"))
        .withColumn("height",     col("height").cast("double"))
        .withColumn("weight",     col("weight").cast("double"))
        .withColumn("birth_date", to_date(col("birth_date"), "yyyy-M-d"))
    )

    # ----------------------------
    # 6. Derive Age from birthDate (more reliable than raw age field)
    # ----------------------------
    df_users = df_users.withColumn(
        "age_derived",
        floor(months_between(current_date(), col("birth_date")) / 12).cast("integer")
    )

    # ----------------------------
    # 7. Normalize String Fields
    # ----------------------------
    df_users = (
        df_users
        .withColumn("first_name",        trim(col("first_name")))
        .withColumn("last_name",         trim(col("last_name")))
        .withColumn("email",             lower(trim(col("email"))))
        .withColumn("gender",            lower(trim(col("gender"))))
        .withColumn("role",              lower(trim(col("role"))))
        .withColumn("address_city",      trim(col("address_city")))
        .withColumn("address_country",   trim(col("address_country")))
        .withColumn("company_name",      trim(col("company_name")))
        .withColumn("company_department",trim(col("company_department")))
    )

    log_quality_metric(report, "02_after_cleaning", df_users, "After flattening, typing, and normalization")
    report["null_summary_after_cleaning"] = compute_null_summary(
        df_users.select("id", "email", "first_name", "last_name", "age", "birth_date", "gender")
    )

    # ----------------------------
    # 8. Deduplicate
    # ----------------------------
    before = df_users.count()
    df_users = df_users.dropDuplicates(["id"])
    report["records_dropped"]["duplicates"] = before - df_users.count()
    log_quality_metric(report, "03_after_dedup", df_users, "After deduplication on id")

    # ----------------------------
    # 9. Data Quality Filters
    # ----------------------------
    before = df_users.count()
    df_users = (
        df_users
        .filter(col("id").isNotNull())
        .filter(col("email").isNotNull() & col("email").contains("@"))
        .filter(col("age_derived").between(16, 100))
        .filter(col("gender").isin("male", "female", "other"))
    )
    report["records_dropped"]["failed_quality_filters"] = before - df_users.count()
    log_quality_metric(report, "04_after_quality_filters", df_users, "After business rule filters")

    # ----------------------------
    # 10. Metadata
    # ----------------------------
    df_users = (
        df_users
        .withColumn("silver_processed_at", current_timestamp())
        .withColumn("source_layer", lit("bronze"))
    )

    report["records_dropped"]["total"] = (
        report["records_dropped"]["duplicates"] +
        report["records_dropped"]["failed_quality_filters"]
    )

    # ----------------------------
    # 11. Write Silver — partitioned by country
    # ----------------------------
    (
        df_users
        .write
        .mode("overwrite")
        .partitionBy("address_country")
        .parquet("../data/silver/users")
    )

    log_quality_metric(report, "05_written_to_silver", df_users, "Final count written to silver")
    save_quality_report(report, "users_silver")
    print(f"Silver users complete. {report['records_dropped']['total']} records dropped.")
    spark.stop()


if __name__ == "__main__":
    transform_users()