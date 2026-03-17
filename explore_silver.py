"""
explore_silver.py
-----------------
DuckDB exploration script for the Silver layer of the medallion e-commerce pipeline.
No pandas required — uses DuckDB's native fetchall() + a pretty-print helper.

Matches actual silver structure:
    data/silver/products/     (partitioned by category)
    data/silver/users/        (partitioned by address_country)
    data/silver/cart_items/   (partitioned by user_id)

Usage:
    pip install duckdb
    python explore_silver.py
"""

import duckdb


# ─────────────────────────────────────────────
# Pretty-print helper (no pandas needed)
# ─────────────────────────────────────────────

def show(rel):
    """Print a DuckDB relation as an aligned table."""
    rows = rel.fetchall()
    cols = [d[0] for d in rel.description]

    if not rows:
        print("  (no rows)")
        return

    widths = [len(c) for c in cols]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val) if val is not None else "NULL"))

    fmt = "  " + "  ".join(f"{{:<{w}}}" for w in widths)
    sep = "  " + "  ".join("-" * w for w in widths)

    print(fmt.format(*cols))
    print(sep)
    for row in rows:
        print(fmt.format(*[str(v) if v is not None else "NULL" for v in row]))


# ─────────────────────────────────────────────
# Connect and register silver views
# ─────────────────────────────────────────────

con = duckdb.connect()

con.execute("""
    CREATE VIEW products   AS SELECT * FROM read_parquet('data/silver/products/**/*.parquet');
    CREATE VIEW users      AS SELECT * FROM read_parquet('data/silver/users/**/*.parquet');
    CREATE VIEW cart_items AS SELECT * FROM read_parquet('data/silver/cart_items/**/*.parquet');
""")

print("=" * 60)
print("SILVER LAYER — DuckDB EXPLORER")
print("=" * 60)


# ─────────────────────────────────────────────
# 1. Health check
# ─────────────────────────────────────────────

print("\n[HEALTH CHECK] Row and column counts\n")

for tbl in ["products", "users", "cart_items"]:
    row_count = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    col_count = con.execute(
        f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{tbl}'"
    ).fetchone()[0]
    print(f"  {tbl:20s}  rows={row_count:>5}  cols={col_count}")


# ─────────────────────────────────────────────
# 2. Schema inspection
# ─────────────────────────────────────────────

print("\n[SCHEMA] All three tables\n")

for tbl in ["products", "users", "cart_items"]:
    print(f"  -- {tbl} --")
    rows = con.execute(f"DESCRIBE {tbl}").fetchall()
    for col_name, col_type, *_ in rows:
        print(f"    {col_name:35s} {col_type}")
    print()


# ─────────────────────────────────────────────
# 3. PRODUCTS
# ─────────────────────────────────────────────

print("=" * 60)
print("PRODUCTS")
print("=" * 60)

print("\n[QUALITY AUDIT]\n")
show(con.execute("""
    SELECT
        COUNT(*)                      AS total,
        COUNT_IF(brand = 'unknown')   AS unknown_brand,
        COUNT_IF(stock = 0)           AS out_of_stock,
        COUNT_IF(rating = 0)          AS unrated,
        COUNT_IF(weight = 0)          AS no_weight
    FROM products
"""))

print("\n[CATEGORY DISTRIBUTION]\n")
show(con.execute("""
    SELECT
        category,
        COUNT(*)                    AS product_count,
        ROUND(AVG(price), 2)        AS avg_price,
        ROUND(AVG(rating), 2)       AS avg_rating,
        SUM(stock)                  AS total_stock
    FROM products
    GROUP BY category
    ORDER BY product_count DESC
"""))

print("\n[TOP 5 MOST EXPENSIVE PRODUCTS]\n")
show(con.execute("""
    SELECT id, title, category, price, stock, rating
    FROM products
    ORDER BY price DESC
    LIMIT 5
"""))


# ─────────────────────────────────────────────
# 4. USERS
# ─────────────────────────────────────────────

print("\n" + "=" * 60)
print("USERS")
print("=" * 60)

print("\n[QUALITY AUDIT — age discrepancy: raw vs derived]\n")
result = con.execute("""
    SELECT
        id,
        first_name,
        age               AS age_raw,
        age_derived,
        age - age_derived AS discrepancy
    FROM users
    WHERE ABS(age - age_derived) > 1
    ORDER BY ABS(age - age_derived) DESC
    LIMIT 10
""")
rows = result.fetchall()
if rows:
    result.description  # already consumed, re-run
    show(con.execute("""
        SELECT id, first_name,
               age AS age_raw, age_derived,
               age - age_derived AS discrepancy
        FROM users
        WHERE ABS(age - age_derived) > 1
        ORDER BY ABS(age - age_derived) DESC
        LIMIT 10
    """))
else:
    print("  No discrepancies found.")

print("\n[GENDER DISTRIBUTION]\n")
show(con.execute("""
    SELECT gender, COUNT(*) AS count
    FROM users
    GROUP BY gender
    ORDER BY count DESC
"""))

print("\n[TOP 10 COUNTRIES BY USER COUNT]\n")
show(con.execute("""
    SELECT
        address_country,
        COUNT(*) AS users
    FROM users
    GROUP BY address_country
    ORDER BY users DESC
    LIMIT 10
"""))

print("\n[ROLE DISTRIBUTION]\n")
show(con.execute("""
    SELECT role, COUNT(*) AS count
    FROM users
    GROUP BY role
    ORDER BY count DESC
"""))


# ─────────────────────────────────────────────
# 5. CART_ITEMS
# ─────────────────────────────────────────────

print("\n" + "=" * 60)
print("CART ITEMS")
print("=" * 60)

print("\n[FLOAT PRECISION AUDIT — API total vs recomputed]\n")
precision_rows = con.execute("""
    SELECT
        cart_id, product_id, product_title,
        line_total, line_total_recomputed, line_total_discrepancy
    FROM cart_items
    WHERE ABS(line_total_discrepancy) > 0.001
    ORDER BY ABS(line_total_discrepancy) DESC
    LIMIT 10
""").fetchall()
if precision_rows:
    show(con.execute("""
        SELECT cart_id, product_id, product_title,
               line_total, line_total_recomputed, line_total_discrepancy
        FROM cart_items
        WHERE ABS(line_total_discrepancy) > 0.001
        ORDER BY ABS(line_total_discrepancy) DESC
        LIMIT 10
    """))
else:
    print("  No significant discrepancies found.")

print("\n[TOP 10 PRODUCTS BY TOTAL QUANTITY SOLD]\n")
show(con.execute("""
    SELECT
        product_id,
        product_title,
        SUM(quantity)             AS total_qty_sold,
        COUNT(DISTINCT cart_id)   AS carts_appeared_in,
        ROUND(AVG(unit_price), 2) AS avg_unit_price
    FROM cart_items
    GROUP BY product_id, product_title
    ORDER BY total_qty_sold DESC
    LIMIT 10
"""))

print("\n[CART SUMMARY — derived from cart_items]\n")
show(con.execute("""
    SELECT
        COUNT(DISTINCT cart_id)              AS total_carts,
        ROUND(AVG(items_per_cart), 1)        AS avg_products_per_cart,
        ROUND(AVG(qty_per_cart), 1)          AS avg_quantity_per_cart,
        ROUND(AVG(cart_total), 2)            AS avg_cart_total,
        ROUND(AVG(cart_discounted_total), 2) AS avg_after_discount
    FROM (
        SELECT
            cart_id,
            COUNT(DISTINCT product_id)         AS items_per_cart,
            SUM(quantity)                      AS qty_per_cart,
            SUM(line_total)                    AS cart_total,
            SUM(line_discounted_total)         AS cart_discounted_total
        FROM cart_items
        GROUP BY cart_id
    )
"""))


# ─────────────────────────────────────────────
# 6. GOLD PREVIEW — cross-table joins
# ─────────────────────────────────────────────

print("\n" + "=" * 60)
print("GOLD PREVIEW — ANALYTICAL JOINS")
print("=" * 60)

print("\n[REVENUE BY PRODUCT CATEGORY]\n")
show(con.execute("""
    SELECT
        p.category,
        COUNT(DISTINCT ci.cart_id)              AS carts,
        SUM(ci.quantity)                        AS units_sold,
        ROUND(SUM(ci.line_discounted_total), 2) AS revenue,
        ROUND(AVG(p.rating), 2)                 AS avg_product_rating
    FROM cart_items ci
    JOIN products p ON ci.product_id = p.id
    GROUP BY p.category
    ORDER BY revenue DESC
"""))

print("\n[CUSTOMER LIFETIME VALUE — TOP 10 SPENDERS]\n")
show(con.execute("""
    SELECT
        u.id                                    AS user_id,
        u.first_name || ' ' || u.last_name      AS full_name,
        u.address_country,
        COUNT(DISTINCT ci.cart_id)              AS total_carts,
        SUM(ci.quantity)                        AS total_items_bought,
        ROUND(SUM(ci.line_discounted_total), 2) AS total_spend
    FROM users u
    JOIN cart_items ci ON u.id = ci.user_id
    GROUP BY u.id, u.first_name, u.last_name, u.address_country
    ORDER BY total_spend DESC
    LIMIT 10
"""))

print("\n[BASKET SIZE VS USER AGE GROUP]\n")
show(con.execute("""
    SELECT
        CASE
            WHEN u.age_derived BETWEEN 16 AND 25 THEN '16-25'
            WHEN u.age_derived BETWEEN 26 AND 35 THEN '26-35'
            WHEN u.age_derived BETWEEN 36 AND 50 THEN '36-50'
            ELSE '51+'
        END                                     AS age_group,
        COUNT(DISTINCT ci.cart_id)              AS carts,
        ROUND(AVG(qty_per_cart), 1)             AS avg_basket_size,
        ROUND(AVG(spend_per_cart), 2)           AS avg_cart_value
    FROM users u
    JOIN (
        SELECT
            user_id, cart_id,
            SUM(quantity)              AS qty_per_cart,
            SUM(line_discounted_total) AS spend_per_cart
        FROM cart_items
        GROUP BY user_id, cart_id
    ) ci ON u.id = ci.user_id
    GROUP BY age_group
    ORDER BY age_group
"""))

print("\n[STOCK RISK — POPULAR PRODUCTS NEARLY OUT OF STOCK]\n")
stock_rows = con.execute("""
    SELECT
        p.id, p.title, p.category, p.stock,
        SUM(ci.quantity)           AS pending_demand,
        p.stock - SUM(ci.quantity) AS net_stock
    FROM products p
    JOIN cart_items ci ON p.id = ci.product_id
    GROUP BY p.id, p.title, p.category, p.stock
    HAVING net_stock < 5
    ORDER BY net_stock ASC
""").fetchall()
if stock_rows:
    show(con.execute("""
        SELECT p.id, p.title, p.category, p.stock,
               SUM(ci.quantity)           AS pending_demand,
               p.stock - SUM(ci.quantity) AS net_stock
        FROM products p
        JOIN cart_items ci ON p.id = ci.product_id
        GROUP BY p.id, p.title, p.category, p.stock
        HAVING net_stock < 5
        ORDER BY net_stock ASC
    """))
else:
    print("  No stock risk detected.")

print("\n" + "=" * 60)
print("Done.")
print("=" * 60)

con.close()