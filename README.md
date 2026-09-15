# Mutual Fund Data Engineering Project

## 📌 Project Overview

This project is an end to end Mutual Fund Data Engineering and Analysis project built using Python, Pandas, NumPy, Amazon S3, Snowflake, and SQL.

The project takes mutual fund data performs data cleaning and transformation using Python, calculates performance metrics and rankings, stores the raw data in Amazon S3, loads the final processed dataset into Snowflake and performs SQL based analysis to generate useful insights.

---

## 🏗️ Project Architecture

```text
                 Mutual Fund CSV
                       │
                       ▼
              Python / Pandas / NumPy
                       │
                       ▼
          Data Cleaning & Transformation
                       │
                       ▼
          Mutual Fund Ranking & Scoring
                       │
                       ▼
             Final Processed CSV
                       │
                       ▼
                 AWS S3 Bucket
                       │
                       ▼
               Snowflake Stage
                       │
                       ▼
             Snowflake Table
                       │
                       ▼
                 SQL Analysis
                       │
                       ▼
              Insights & Rankings


              ## 🛠️ Technologies Used



### Programming & Data Processing
- Python
- Pandas
- NumPy

### AWS
- Amazon S3
- AWS Glue Crawler
- AWS Glue
- IAM

### Data Warehouse & SQL
- Snowflake
- SQL

### Version Control
- Git
- GitHub


---

## 📊 Dataset

The dataset contains mutual fund information including:

- Fund Name
- Category
- 1-Year Return
- 3-Year Return
- 5-Year Return
- Expense Ratio
- Risk Rating
- AUM (Assets Under Management)

The final processed dataset also contains calculated ranking and scoring columns.

---

## 🐍 Python Data Processing

Python was used for:

- Loading the mutual fund dataset
- Data cleaning and validation
- Duplicate checking
- Missing value validation
- Data type validation
- Return calculations
- Performance scoring
- Expense ranking
- Risk scoring
- Final ranking
- Exporting the processed dataset

The final processed file is:

`output/final_mutual_fund_ranking.csv`

---

## ☁️ AWS S3

Amazon S3 was used as the cloud storage layer.

### S3 Bucket Structure

```text
mutual-fund-data-engineering-bhagvat-2026/
│
├── raw/
│   └── mutual_funds.csv
│
├── processed/
│
└── curated/


❄️ Snowflake

Snowflake was used as the cloud data warehouse for storing and analyzing the processed mutual fund data.

Database

SNOWFLAKE_LEARNING_DB

Schema

MUTUAL_FUND

Table

MUTUAL_FUNDS

The final processed CSV was loaded into Snowflake.

The table contains 16 columns covering:

Fund information
Returns
Expense ratio
Risk rating
AUM
Average return
Performance rank
Expense rank
Risk score
Return score
Expense score
Performance score
Final rank

The table contains 15 mutual fund records.