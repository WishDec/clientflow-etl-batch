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
