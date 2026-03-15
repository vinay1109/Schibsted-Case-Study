# Case Study : User Interest Pipeline

## Overview

A lightweight data pipeline built on the [FakeStore API](https://fakestoreapi.com/) that models user shopping behavior. 

This project shows how a simple API dataset can be transformed into useful analytical and feature-oriented datasets using a structured data pipeline.

The implementation is intentionally lightweight, but the design reflects common patterns used in modern data platforms:

- layered data modeling -  **raw → stage → enrich → curate** architecture.
- reusable enrichment logic
- consumer-specific curated outputs
- clear path to production deployment

---

## How to Run
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python3 main.py
```

Dependencies: `pandas`, `duckdb`, `requests`

---

## Pipeline Structure
```
Fetch FakeStore API Response (/users, /products, /carts)
     |
Raw Layer       -> data/raw/*.json
     |
Stage Layer     -> users_stg, products_stg, cart_items_stg
     |
Enrich Layer    -> cart_items_enriched (cart + product join)
     |
Curate Layer    -> category_popularity_summary
                -> user_interest_features
```

---

## Outputs

**`category_popularity_summary`** — aggregated category performance (total items sold, revenue, unique users). Useful for dashboards and product analytics.

**`user_interest_features`** — user-level behavioral features (total spend, cart count, top category, premium spend ratio, recency). Designed for segmentation and ML use cases.

All outputs are saved as `.csv` and `.parquet` under `data/curate/`.

---

## Assumptions & Shortcuts

- **Local storage** - Raw data is stored locally rather than in object storage such as S3.
- **DuckDB** - DuckDB is used as a lightweight analytical engine instead of a cloud warehouse like Snowflake.
- **No orchestration** — The pipeline is executed as a single python script rather than orchestrated through Airflow.
- **Minimal validation** — The pipeline assumes that the FakeStore API responses are well-formed.
- **Full reload** — No incremental processing; each run replaces all data
- **Batch execution** - The pipeline runs as a batch job instead of a real-time streaming pipeline.

---

## Production Design

In production this pipeline would run as a daily scheduled job:

- **Ingestion**: scheduled API extraction (Airflow or EventBridge)
- **Storage**: raw snapshots in S3
- **Warehouse**: Snowflake with dbt models for each layer
- **Monitoring**: freshness checks, row count anomaly detection, Slack alerts on failure
- **Observability**: structured logging to log tables using cloudwatch logs

The raw -> stage -> enrich -> curate pattern will stay the same.

---

## Monitoring Consideration

One important metric to monitor would be **data freshness**.

Example checks:

- Has the pipeline run successfully today?
- Did the number of carts processed change significantly?
- Are new categories appearing unexpectedly?

Which helps to ensure downstream consumers always receive reliable data.

---

## What I'd Improve Before Going Live

1. **Data quality checks** — non-null IDs, non-negative quantities, valid prices
2. **Incremental processing** — ingest only new/updated records
3. **Exactly-One processing** — safe to re-run without duplicating data
4. **Schema contracts** — explicit schemas per layer to protect downstream consumers

---

## ML Extension

`user_interest_features` is already structured as an ML-ready dataset:

| Feature | Signal |
|---|---|
| `total_spend`, `avg_cart_value` | Purchase intent / value |
| `top_category_by_spend` | Category affinity |
| `distinct_categories` | Variety of categories |
| `days_since_last_cart` | Freshness |
| `premium_spend_ratio` | Price sensitivity |

**How I'd serve it**: For a batch ML use case, I would store features in a curated snowflake table. 
If low-latency online inference were needed, I would keep snowflake as the offline feature store and introduce a separate online serving layer.

**Pipeline change**: Add a feature store write step at the end of the curate layer - no structural changes needed.
