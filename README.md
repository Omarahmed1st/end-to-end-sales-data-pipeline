🚀 End-to-End Sales Data Engineering Pipeline PySpark • PostgreSQL • KPI
Data Mart • Analytical SQL

============================================================

📌 Project Overview

This project implements a complete end-to-end data engineering pipeline
processing 100,000+ sales records using PySpark for distributed data
processing and PostgreSQL for structured analytical reporting.

Workflow: - Raw data ingestion - Data cleaning & transformation -
Feature engineering - KPI aggregation - Data mart design - Business
analytics layer

============================================================

🏗 System Architecture

Raw CSV Dataset (100k rows) │ ▼ PySpark ETL Layer - Data cleaning - Date
standardization - Feature engineering - KPI computation │ ▼ KPI Data
Export │ ▼ PostgreSQL Data Mart (kpi schema) │ ▼ Analytical SQL Queries

============================================================

⚙ ETL Layer (PySpark)

The ETL pipeline performs: - Schema inference and column normalization -
Date parsing & validation - Profit margin calculation - Time-based
feature generation (year, month) - KPI aggregations using Spark
transformations

Generated KPIs: - Total revenue & total profit - Monthly revenue and
profit trends - Channel performance analysis - Top-performing
countries - Item type profitability - Revenue distribution by order
priority

============================================================

🗄 Data Mart Design (PostgreSQL)

Schema: kpi

Tables: - summary - monthly_kpi - top_countries - channel_kpi -
item_kpi - priority_kpi

============================================================

🔍 Analytical SQL Example

SELECT sales_channel, revenue, ROUND( ((revenue / SUM(revenue) OVER
()) * 100)::numeric, 2 ) AS revenue_share_pct FROM kpi.channel_kpi;

============================================================

🛠 Technologies Used

-   PySpark
-   PostgreSQL
-   SQL (CTEs, Window Functions, Aggregations)
-   Pandas
-   pgAdmin
-   Git

============================================================

💼 Skills Demonstrated

✔ Data Engineering Workflow
✔ Distributed Data Processing
✔ KPI Data Mart Design
✔ Advanced SQL Analytics
✔ Business-Oriented Data Interpretation

============================================================

Author: Omar ahmed Data Engineer | SQL & Spark Specialist
