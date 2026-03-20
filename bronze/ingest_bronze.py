"""
ingest_bronze.py
----------------
Fetches raw data from the DummyJSON API and saves it to the Bronze layer.
Handles pagination so you get the full dataset, not just the default first page.

Endpoints:
    /products  → data/bronze/products/products_raw.json
    /users     → data/bronze/users/users_raw.json
    /carts     → data/bronze/carts/carts_raw.json

Usage (standalone):
    python ingest_bronze.py

Usage (called by Airflow):
    from ingest_bronze import ingest_all
    ingest_all()
"""

import requests
import json
import os
from datetime import datetime

BASE_URL = "https://dummyjson.com"
BRONZE_PATH = "data/bronze"

# DummyJSON returns 30 items per page by default.
# Setting limit=0 returns everything in one call.
ENDPOINTS = {
    "products": f"{BASE_URL}/products?limit=0",
    "users":    f"{BASE_URL}/users?limit=0",
    "carts":    f"{BASE_URL}/carts?limit=0",
}


def fetch(url: str) -> dict:
    """Fetch JSON from a URL with basic error handling."""
    print(f"  Fetching: {url}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()  # raises on 4xx / 5xx
    return response.json()


def save(data: dict, entity: str) -> str:
    """Save raw JSON to the Bronze layer. Returns the file path."""
    folder = os.path.join(BRONZE_PATH, entity)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{entity}_raw.json")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"  Saved {len(data.get(entity, []))} {entity} → {path}")
    return path


def ingest_all() -> dict:
    """
    Fetch all three endpoints and save to Bronze.
    Returns a summary dict (used by the Airflow DAG to push to XCom).
    """
    summary = {
        "run_timestamp": datetime.now().isoformat(),
        "entities": {}
    }

    print(f"\n[Bronze Ingestion] Starting at {summary['run_timestamp']}\n")

    for entity, url in ENDPOINTS.items():
        try:
            data = fetch(url)
            path = save(data, entity)
            record_count = len(data.get(entity, []))
            summary["entities"][entity] = {
                "status": "ok",
                "record_count": record_count,
                "path": path
            }
        except Exception as e:
            summary["entities"][entity] = {
                "status": "failed",
                "error": str(e)
            }
            print(f"  ERROR fetching {entity}: {e}")
            raise  # re-raise so Airflow marks the task as failed

    # Save ingestion manifest to Bronze for traceability
    manifest_path = os.path.join(BRONZE_PATH, "ingestion_manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n[Bronze Ingestion] Complete. Manifest → {manifest_path}")
    return summary


if __name__ == "__main__":
    ingest_all()