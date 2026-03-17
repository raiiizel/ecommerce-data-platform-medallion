import requests
import json
from pathlib import Path

BASE_URL = "https://dummyjson.com"

ENDPOINTS = {
    "products": "/products",
    "users": "/users",
    "carts": "/carts"
}

BRONZE_PATH = Path("data/bronze")


def fetch_data(endpoint):
    url = BASE_URL + endpoint
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def save_raw(entity, data):
    path = BRONZE_PATH / entity
    path.mkdir(parents=True, exist_ok=True)

    file_path = path / f"{entity}_raw.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)


def main():

    for entity, endpoint in ENDPOINTS.items():

        print(f"Ingesting {entity}...")

        data = fetch_data(endpoint)

        save_raw(entity, data)

    print("Bronze ingestion completed.")


if __name__ == "__main__":
    main()