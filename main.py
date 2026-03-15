from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import requests

BASE_URL = "https://fakestoreapi.com"
ROOT = Path(__file__).parent
RAW_DIR = ROOT / "data" / "raw"
STAGE_DIR = ROOT / "data" / "stage"
ENRICH_DIR = ROOT / "data" / "enrich"
CURATE_DIR = ROOT / "data" / "curate"
WAREHOUSE_DIR = ROOT / "data" / "warehouse"
SQL_DIR = ROOT / "sql"
DB_PATH = WAREHOUSE_DIR / "case_study.duckdb"


def validate_dirs() -> None:
    for path in [RAW_DIR, STAGE_DIR, ENRICH_DIR, CURATE_DIR, WAREHOUSE_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def fetch_endpoint(endpoint: str) -> list[dict[str, Any]]:
    url = f"{BASE_URL}/{endpoint}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def save_raw_json(data: list[dict[str, Any]], name: str, ingestion_ts: str) -> None:
    payload = {
        "endpoint": name,
        "ingested_at": ingestion_ts,
        "record_count": len(data),
        "data": data,
    }
    with open(RAW_DIR / f"{name}.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def normalize_users(users: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for user in users:
        rows.append(
            {
                "user_id": user.get("id"),
                "email": user.get("email"),
                "username": user.get("username"),
                "firstname": user.get("name", {}).get("firstname"),
                "lastname": user.get("name", {}).get("lastname"),
                "city": user.get("address", {}).get("city"),
                "zipcode": user.get("address", {}).get("zipcode"),
                "lat": user.get("address", {}).get("geolocation", {}).get("lat"),
                "lon": user.get("address", {}).get("geolocation", {}).get("long"),
            }
        )
    return pd.DataFrame(rows)


def normalize_products(products: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for product in products:
        rows.append(
            {
                "product_id": product.get("id"),
                "title": product.get("title"),
                "price": product.get("price"),
                "category": product.get("category"),
                "rating_rate": product.get("rating", {}).get("rate"),
                "rating_count": product.get("rating", {}).get("count"),
            }
        )
    return pd.DataFrame(rows)


def normalize_cart(carts: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for cart in carts:
        for item in cart.get("products", []):
            rows.append(
                {
                    "cart_id": cart.get("id"),
                    "user_id": cart.get("userId"),
                    "cart_date": cart.get("date"),
                    "product_id": item.get("productId"),
                    "quantity": item.get("quantity"),
                }
            )
    return pd.DataFrame(rows)


def run_sql(con: duckdb.DuckDBPyConnection, filename: str) -> None:
    sql_path = SQL_DIR / filename
    print(f"Running SQL file: {sql_path}")
    sql = sql_path.read_text(encoding="utf-8")
    con.execute(sql)


def export_table(con: duckdb.DuckDBPyConnection, table_name: str ,output_dir: Path,) -> None:
    csv_path = output_dir / f"{table_name}.csv"
    parquet_path = output_dir / f"{table_name}.parquet"

    con.execute(
        f"COPY {table_name} TO '{csv_path.as_posix()}' "
        f"(HEADER, DELIMITER ',');"
    )
    con.execute(
        f"COPY {table_name} TO '{parquet_path.as_posix()}' "
        f"(FORMAT PARQUET);"
    )


def main() -> None:
    validate_dirs()
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    print("Fetching API data...")
    users = fetch_endpoint("users")
    products = fetch_endpoint("products")
    carts = fetch_endpoint("carts")

    print("Saving raw JSON...")
    save_raw_json(users, "users", ingestion_ts)
    save_raw_json(products, "products", ingestion_ts)
    save_raw_json(carts, "carts", ingestion_ts)

    print("Normalizing source datasets...")
    users_df = normalize_users(users)
    products_df = normalize_products(products)
    cart_items_df = normalize_cart(carts)

    print("Building DuckDB warehouse...")
    con = duckdb.connect(DB_PATH.as_posix())

    con.register("users_df", users_df)
    con.register("products_df", products_df)
    con.register("cart_items_df", cart_items_df)


    print("Running stage...")
    run_sql(con, "user_stg.sql")
    run_sql(con, "product_stg.sql")
    run_sql(con, "cart_items_stg.sql")

    print("Exporting stage layer...")
    export_table(con, "users_stg", STAGE_DIR)
    export_table(con, "products_stg", STAGE_DIR)
    export_table(con, "cart_items_stg", STAGE_DIR)

    print("Running transformations...")
    run_sql(con, "cart_items_enriched.sql")
    run_sql(con, "category_popularity_summary.sql")
    run_sql(con, "user_interest_features.sql")



    print("Exporting enrich layer...")
    export_table(con, "cart_items_enriched", ENRICH_DIR)

    print("Exporting curated outputs...")
    export_table(con, "category_popularity_summary", CURATE_DIR)
    export_table(con, "user_interest_features", CURATE_DIR)

    summary = con.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM users_stg) AS users_count,
            (SELECT COUNT(*) FROM products_stg) AS products_count,
            (SELECT COUNT(*) FROM cart_items_stg) AS cart_items_count,
            (SELECT COUNT(*) FROM cart_items_enriched) AS cart_items_enriched_count,
            (SELECT COUNT(*) FROM category_popularity_summary) AS category_summary_rows,
            (SELECT COUNT(*) FROM user_interest_features) AS user_feature_rows
        """
    ).fetchdf()

    print("\nPipeline completed successfully.\n")
    print(summary.to_string(index=False))
    print(f"\nDuckDB database created at: {DB_PATH}")
    print(f"Styage output written to: {ENRICH_DIR}")
    print(f"Enrich output written to: {ENRICH_DIR}")
    print(f"Curated outputs written to: {CURATE_DIR}")

    con.close()


if __name__ == "__main__":
    main()