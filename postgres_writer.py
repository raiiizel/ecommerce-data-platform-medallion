"""
postgres_writer.py
------------------
Shared utility for writing PySpark Gold DataFrames into Postgres.
Used by all three Gold transforms.

The ecommerce_dw database must be running (docker compose up postgres).

Connection config is read from environment variables with sensible
defaults matching the docker-compose.yml setup:

    POSTGRES_HOST     default: localhost
    POSTGRES_PORT     default: 5432
    POSTGRES_DB       default: ecommerce_dw
    POSTGRES_USER     default: admin
    POSTGRES_PASSWORD default: admin
"""

import os
from pyspark.sql import DataFrame


def get_jdbc_config() -> dict:
    """
    Returns JDBC connection config from env vars (or defaults).
    Centralised here so all Gold transforms stay in sync.
    """
    host     = os.getenv("POSTGRES_HOST",     "localhost")
    port     = os.getenv("POSTGRES_PORT",     "5432")
    db       = os.getenv("POSTGRES_DB",       "ecommerce_dw")
    user     = os.getenv("POSTGRES_USER",     "admin")
    password = os.getenv("POSTGRES_PASSWORD", "admin")

    return {
        "url":      f"jdbc:postgresql://{host}:{port}/{db}",
        "user":     user,
        "password": password,
        "driver":   "org.postgresql.Driver",
    }


def write_to_postgres(df: DataFrame, table: str, mode: str = "overwrite") -> None:
    """
    Write a PySpark DataFrame to a Postgres table via JDBC.

    Args:
        df:    the DataFrame to write
        table: target table name in ecommerce_dw (e.g. "gold_customer_360")
        mode:  "overwrite" (default) replaces the table each pipeline run
               "append"    adds rows without truncating
    """
    config = get_jdbc_config()

    print(f"  Writing {df.count()} rows → postgres:{table} (mode={mode})")

    (
        df.write
        .format("jdbc")
        .option("url",      config["url"])
        .option("dbtable",  table)
        .option("user",     config["user"])
        .option("password", config["password"])
        .option("driver",   config["driver"])
        # Batch size: how many rows are sent to Postgres per round trip.
        # 1000 is a safe default — increase for larger datasets.
        .option("batchsize", "1000")
        .mode(mode)
        .save()
    )

    print(f"  Done → {table}")