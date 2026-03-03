# ClientFlow ETL (Batch Pipeline)

## Overview

This project demonstrates a batch ETL pipeline designed for marketplace pricing and margin analytics.

The pipeline simulates data ingestion from marketplace sources (e.g., Kaspi-like platform), processes pricing data, and builds analytical datasets for business insights.

The architecture follows a layered data approach:

- Raw Layer (data ingestion)
- Staging Layer (data cleaning & normalization)
- Data Mart Layer (business metrics & aggregations)

---

## Business Context

ClientFlow is a profit & pricing intelligence platform.

The goal of this ETL pipeline is to:

- Ingest product price data
- Calculate margins
- Track price changes
- Build analytical tables for reporting
- Detect abnormal pricing behavior

---

## Architecture

Batch Processing Flow:

1. Extract marketplace pricing data (CSV / API simulation)
2. Transform raw data (cleaning, normalization, validation)
3. Load processed data into analytical tables
4. Generate business metrics

---

## Tech Stack

- Python (ETL logic)
- SQL (data transformations)
- Relational Database (PostgreSQL-style modeling)
- Structured Layered Architecture

---
## Project Structure

```
clientflow-etl-batch/
│
├── etl/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│
├── sql/
│   ├── create_tables.sql
│   ├── transformations.sql
│
└── README.md
```
---

## Status

This repository demonstrates ETL design and data architecture principles for batch analytics pipelines.

## How to Use (Demo)

This repository can be reviewed directly in the browser.

If you run it locally (optional), the demo flow is:

1) Extract demo CSV into raw-like records:
- `etl/extract.py`

2) Transform into staging (validation & normalization):
- `etl/transform.py`

3) Build daily analytics in-memory (mart-like output):
- `etl/load.py`

Demo input data:
- `data/sample_offers.csv`

SQL reference model:
- `sql/create_tables.sql`
- `sql/transformations.sql`

---

## Data Model (Layers)

- `raw.marketplace_offers`  
  Raw ingested marketplace offers (append-only, includes raw_payload)

- `stg.offers_clean`  
  Cleaned and validated offers with business date + validation flags

- `mart.product_price_daily`  
  Daily per-product metrics (best price, average, offers count)

- `mart.price_anomalies`  
  Example anomaly detection (sudden drops)

---

## What This Demonstrates

- Batch ETL design (RAW → STG → MART)
- Data validation rules (bad price handling)
- Analytical modeling for pricing & margin use-cases
- Production-like structure: readable modules + SQL transformations
- ClientFlow domain alignment (pricing intelligence & profitability)
