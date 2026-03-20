"""
ecommerce_pipeline_dag.py
--------------------------
Airflow DAG for the full Medallion Architecture e-commerce pipeline.
PySpark runs locally on the host machine via the venv Python.

Flow:
    start
      ↓
    bronze_ingest
      ↓  (fan-out)
    silver_products ── silver_users ── silver_carts
      ↓  (fan-in / fan-out)
    gold_customer_360 ── gold_product_performance ── gold_stock_risk
      ↓  (fan-in)
    pipeline_done
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# ─────────────────────────────────────────────
# Paths — adjust if your project lives elsewhere
# ─────────────────────────────────────────────
PYTHON  = r"C:\Users\ismai\OneDrive\Documents\projects\ecommerce-data-platform-medallion\venv\Scripts\python.exe"
PROJECT = r"C:\Users\ismai\OneDrive\Documents\projects\ecommerce-data-platform-medallion"

def py(script: str) -> str:
    """Build the bash command to run a script with the venv Python."""
    return f'"{PYTHON}" "{PROJECT}\\{script}"'


DEFAULT_ARGS = {
    "owner": "raizel",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_medallion_pipeline",
    description="Bronze → Silver → Gold pipeline for the e-commerce dataset",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",   # daily at 06:00 UTC
    catchup=False,           # don't backfill missed runs
    max_active_runs=1,       # only one run at a time
    tags=["ecommerce", "medallion", "pyspark"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="pipeline_done")

    # ── Bronze ────────────────────────────────────────────────
    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=py("bronze/ingest_bronze.py"),
    )

    # ── Silver — parallel ─────────────────────────────────────
    silver_products = BashOperator(
        task_id="silver_products",
        bash_command=py("silver/transform_products.py"),
    )
    silver_users = BashOperator(
        task_id="silver_users",
        bash_command=py("silver/transform_users.py"),
    )
    silver_carts = BashOperator(
        task_id="silver_carts",
        bash_command=py("silver/transform_carts.py"),
    )

    # ── Gold — parallel after all silver succeeds ─────────────
    gold_customer_360 = BashOperator(
        task_id="gold_customer_360",
        bash_command=py("gold/transform_gold_customer_360.py"),
    )
    gold_product_performance = BashOperator(
        task_id="gold_product_performance",
        bash_command=py("gold/transform_gold_product_performance.py"),
    )
    gold_stock_risk = BashOperator(
        task_id="gold_stock_risk",
        bash_command=py("gold/transform_gold_stock_risk.py"),
    )

    # ── Dependency graph ──────────────────────────────────────
    start >> bronze_ingest

    # bronze fans out to all three silver tasks
    bronze_ingest >> silver_products
    bronze_ingest >> silver_users
    bronze_ingest >> silver_carts

    # all silver must complete before any gold starts
    silver_products   >> gold_customer_360
    silver_products   >> gold_product_performance
    silver_products   >> gold_stock_risk
    silver_users      >> gold_customer_360
    silver_users      >> gold_product_performance
    silver_users      >> gold_stock_risk
    silver_carts      >> gold_customer_360
    silver_carts      >> gold_product_performance
    silver_carts      >> gold_stock_risk

    # all gold fans into end
    gold_customer_360        >> end
    gold_product_performance >> end
    gold_stock_risk          >> end