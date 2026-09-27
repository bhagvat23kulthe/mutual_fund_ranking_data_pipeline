# Mutual Fund Ranking Pipeline

A cloud-based data pipeline that cleans, scores and ranks mutual funds across multiple categories using returns, expense ratio and risk rating built to simulate a real world, end to end data engineering workflow on AWS.

## Overview

This project takes raw mutual fund data (15 funds across 6 categories) and produces a fair, weighted ranking by normalizing metrics that are otherwise measured on different scales. It also demonstrates a full cloud data pipeline: ingestion, transformation, cataloging, warehousing, and orchestration.

## Tech Stack

- **Languages:** Python, SQL
- **Data Processing:** Pandas, NumPy
- **Cloud Storage:** Amazon S3 (raw, processed, curated folders)
- **Cataloging:** AWS Glue (Crawler, Data Catalog)
- **Data Warehousing:** Snowflake, Amazon Redshift
- **Ad-hoc Querying:** Amazon Athena
- **Orchestration:** AWS Step Functions, Apache Airflow (AWS MWAA)
s
## Pipeline Flow

1. **Ingestion** – Raw mutual fund CSV data is uploaded to Amazon S3 (raw folder).
2. **Cleaning & Validation** – Duplicates removed, data types fixed, missing values handled using Pandas.
3. **Scoring** – A weighted composite score normalizes returns, expense ratio and risk rating onto a common scale for fair cross category comparison.
4. **Cataloging** – An AWS Glue Crawler catalogs the S3 data for querying.
5. **Warehousing** – Final ranked dataset is loaded into Snowflake and Amazon Redshift via S3 stages.
6. **Analytics** – SQL queries (GROUP BY, ORDER BY, aggregations) rank funds, compare categories, and break down individual scores.
7. **Ad-hoc Queries** – Amazon Athena runs direct queries on S3 files without needing a warehouse.
8. **Orchestration** – The entire pipeline is automated end-to-end using AWS Step Functions (with retry/error handling) and an Apache Airflow DAG (AWS MWAA).

## Output

- `Final_Mutual_Fund_Ranking.csv` — final ranked dataset (16 columns, 15 records)
- Category-wise and overall fund rankings
- Analytics on average returns and expense ratios by category

## Key Learnings

- Designing a fair, normalized scoring system across heterogeneous metrics
- Structuring S3 storage using raw/processed/curated conventions
- Building fault-tolerant pipelines with validation and error-handling branches
- Orchestrating multi-step cloud workflows with Step Functions and Airflow
