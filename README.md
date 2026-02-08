# Audience Growth Intelligence Platform (YouTube)

This project implements a snapshot-based OLAP analytics platform that extracts public YouTube engagement data, stores historical snapshots in a PostgreSQL data warehouse, and enables analytical queries and KPI-driven insights on audience growth and content performance.

The focus of this project is data engineering and data warehousing concepts rather than replacing YouTube Studio.

## Project Motivation

YouTube Studio provides ready-made dashboards for creators, but it does not expose the underlying analytical pipeline or allow custom historical data modeling. This project demonstrates how engagement analytics can be engineered from raw public data using an OLAP data warehouse and SQL-based analysis.

The platform is designed to support data-driven decisions for content scaling and localization.
It is designed to be tailored per creator, allowing customer-specific data extraction, KPI definitions, snapshot strategies, and future localization workflows based on individual audience behavior and content goals.


## Project Objectives

The objectives of this project are:

- Extract publicly available YouTube engagement metrics using APIs
- Design and implement a dimensional (star) schema
- Store daily historical snapshots of video performance
- Enable OLAP-style analytical queries
- Compute KPIs such as engagement rate and growth trends
- Demonstrate OLTP to OLAP data flow using a real-world platform

---

## Project Scope

Included in current implementation:
- Public YouTube data via API key
- PostgreSQL-based OLAP data warehouse
- Python ETL using psycopg
- Snapshot-based fact table
- SQL-based analytics and KPIs

Not included in current implementation:
- Private YouTube Studio analytics (OAuth)
- LLMs or voice cloning (planned as future enhancements)

---

## Technology Stack

- Python 3
- psycopg (psycopg3)
- PostgreSQL
- YouTube Data API v3
- pgAdmin 4
- Git and GitHub
- Windows Command Prompt



## Architecture Overview

YouTube Platform (OLTP)
        |
        |
YouTube Data API (Public Metrics)
        |
        |
Python ETL Process
        |
        |
PostgreSQL Data Warehouse (OLAP)
        |
        |
SQL Analytics and KPIs



## Data Warehouse Design

The project uses a star schema optimized for analytical queries.

                    dim_date
                        |
                        |
dim_video ---- fact_video_snapshot_daily ---- dim_channel


Fact table:
- fact_video_snapshot_daily
  - view_count
  - like_count
  - comment_count
  - snapshot date

Dimension tables:
- dim_date for calendar attributes
- dim_video for video metadata
- dim_channel for channel metadata (future-ready design)

This design supports time-series analysis and historical trend evaluation.



## Environment Setup (Windows)

Step 1: Create and activate virtual environment
python -m venv dmenv
dmenv\Scripts\activate

Step 2: Install dependencies
pip install -r requirements.txt

Step 3: Configure environment variables
Create a .env file locally (not committed):


PGHOST=localhost
PGPORT=5432
PGDATABASE=youtube_growth_dw
PGUSER=postgres
PGPASSWORD=YOUR_DB_PASSWORD

YOUTUBE_API_KEY=YOUR_API_KEY
CHANNEL_ID=YOUR_CHANNEL_ID


A template file (.env.example) is provided in the repository.

## Database Initialization

Create the database once in PostgreSQL:
CREATE DATABASE youtube_growth_dw;


Initialize the schema using Python:
python init_db.py


This creates the warehouse schema, dimension tables, and snapshot fact table.


## Running Daily Snapshots

Snapshots preserve historical states of engagement metrics because the YouTube API provides only current values.

Run the snapshot script once per day:
Each execution captures:
- current view count
- like count
- comment count
mapped to the snapshot date.


## Sample Analytical Queries

Latest snapshot join:
SELECT
d.date_value, v.title, f.view_count, f.like_count, f.comment_count
FROM warehouse.fact_video_snapshot_daily f
JOIN warehouse.dim_date d ON d.date_id = f.date_id
JOIN warehouse.dim_video v ON v.video_sk = f.video_sk
ORDER BY d.date_value DESC, f.view_count DESC;

Daily view growth using window function:
SELECT
v.title,
d.date_value,
f.view_count,
f.view_count -
LAG(f.view_count) OVER (
PARTITION BY v.video_sk
ORDER BY d.date_value
) AS daily_view_growth
FROM warehouse.fact_video_snapshot_daily f
JOIN warehouse.dim_date d ON d.date_id = f.date_id
JOIN warehouse.dim_video v ON v.video_sk = f.video_sk;

Engagement rate KPI:
SELECT
v.title,
d.date_value,
ROUND(
((f.like_count + f.comment_count)::numeric
/ NULLIF(f.view_count, 0)) * 100,
2
) AS engagement_rate_pct
FROM warehouse.fact_video_snapshot_daily f
JOIN warehouse.dim_date d ON d.date_id = f.date_id
JOIN warehouse.dim_video v ON v.video_sk = f.video_sk;



## Difference from YouTube Studio

YouTube Studio provides pre-built analytics dashboards for creators.

This project focuses on:
- building the analytical backend
- designing a data warehouse
- storing independent historical snapshots
- defining custom KPIs using SQL

YouTube Studio consumes analytics.
This project constructs analytics.



## Future Enhancements

Multilingual voice conversion with creator authenticity:
The platform can be extended to support creator-authentic multilingual dubbing by combining speech-to-text, style-preserving translation, and consent-based voice cloning. The workflow will first convert Malayalam content to English and then reuse the English version as a pivot for additional languages, while preserving the creator’s voice and speaking style.

Short-form content generation:
Another planned enhancement is automated repurposing of long-form videos into multiple short-form clips suitable for YouTube Shorts. Segment selection can be guided by speech and engagement patterns. Generated shorts can also be localized using the same voice conversion pipeline.

Real-time multilingual live broadcasting:
After validating quality on uploaded videos, the system can be extended to support live multilingual broadcasting using a streaming speech recognition, translation, and voice conversion pipeline, with language-specific live outputs.

All voice-related features will be implemented strictly with explicit creator consent.



## Academic and Portfolio Value

This project demonstrates:
- OLTP to OLAP data flow
- dimensional modeling
- snapshot-based fact table design
- SQL analytics and KPI computation
- real-world API-driven ETL pipelines
- forward-compatible system design


## License

This project is intended for academic and learning purposes.
