import json

# Check products
with open('data/bronze/products/products_raw.json') as f:
    data = json.load(f)
    print(f"Products in raw file: {len(data['products'])}")

# Check users
with open('data/bronze/users/users_raw.json') as f:
    data = json.load(f)
    print(f"Users in raw file: {len(data['users'])}")

# Check carts
with open('data/bronze/carts/carts_raw.json') as f:
    data = json.load(f)
    print(f"Carts in raw file: {len(data['carts'])}")
    total_items = sum(len(c['products']) for c in data['carts'])
    print(f"Total cart items (after explode): {total_items}")
