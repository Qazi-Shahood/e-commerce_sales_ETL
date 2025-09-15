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
      
