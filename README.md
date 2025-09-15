📌 Project Summary

Objective: Build an ETL pipeline for Olist Brazilian E-commerce data with Databricks + PySpark + Delta Lake.

Problems Solved:

Cleaned and standardized multi-source e-commerce data

Created business metrics KPIs: revenue by category, orders by state.

Added geolocation for city/state visualizations

Future Analysis:

Payment method trends

Delivery performance metrics

Review sentiment vs sales

Sales forecasting

Data source: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data/code?select=olist_customers_dataset.csv

                ┌──────────────────────────┐
                │      Data Sources        │
                │ Orders, Items, Products, │
                │ Customers, Payments, Geo │
                └──────────────┬───────────┘
                               │
                               ▼
                      ┌────────────────┐
                      │  Bronze Layer  │
                      │ Raw CSVs as-is │
                      └───────┬────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  Silver Layer    │
                    │ Cleaning &       │
                    │ Standardization  │
                    │ - Valid orders   │
                    │ - Clean products │
                    │ - Aggregate price│
                    └───────┬──────────┘
                            │
                            ▼
                   ┌───────────────────┐
                   │   Gold Layer      │
                   │ Business Insights │
                   │ - Revenue by cat  │
                   │ - Orders by city  │
                   │ - Geo enrichment  │
                   └───────────────────┘

  <img width="1263" height="667" alt="Screenshot 2025-09-16 at 12 21 40 AM" src="https://github.com/user-attachments/assets/2e1cd517-995c-4ace-b72e-fd7a3d7fa39b" />


