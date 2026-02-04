# Audience Growth Intelligence Platform (YouTube)

A snapshot-based analytics pipeline that extracts public YouTube engagement data using the YouTube Data API, loads it into a PostgreSQL dimensional warehouse (OLAP), and enables KPI/trend analysis over time.

## Tech Stack
- Python (ETL)
- psycopg (PostgreSQL driver)
- PostgreSQL (data warehouse)
- YouTube Data API v3 (public data)
- pgAdmin (verification)

## Project Structure (current)
- `init_db.py` – creates schemas and tables (runs `schema.sql`)
- `schema.sql` – warehouse DDL (dim/fact tables)
- `run_snapshot.py` – extracts YouTube video stats and loads daily snapshots
- `.env` – local environment variables (NOT committed)
- `.env.example` – template for environment variables

## Setup (Windows)
### Create and activate venv
```bat
cd C:\Users\Nimmy\Desktop\workspace\dataminingworkspace\audience-growth-intelligence
python -m venv dmenv
dmenv\Scripts\activate
