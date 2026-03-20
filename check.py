import duckdb
import os

ROOT = os.path.dirname(os.path.abspath(__file__))

con = duckdb.connect()
con.execute(f"CREATE VIEW products   AS SELECT * FROM read_parquet('{ROOT}/data/silver/products/**/*.parquet')")
con.execute(f"CREATE VIEW users      AS SELECT * FROM read_parquet('{ROOT}/data/silver/users/**/*.parquet')")
con.execute(f"CREATE VIEW cart_items AS SELECT * FROM read_parquet('{ROOT}/data/silver/cart_items/**/*.parquet')")

print(con.execute("SELECT COUNT(*) FROM products").fetchone()[0],   "products")
print(con.execute("SELECT COUNT(*) FROM users").fetchone()[0],      "users")
print(con.execute("SELECT COUNT(*) FROM cart_items").fetchone()[0], "cart_items rows")