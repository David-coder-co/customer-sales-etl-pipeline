# customer-sales-etl-pipeline
## Overview

Businesses generate sales data every day from various operations, such as in-store purchases, online sales, and internal systems. This data is essential for understanding customer behavior, tracking product performance, and making informed business decisions.

In this project, sales data files are uploaded to Google Cloud Storage (GCS). The pipeline then automatically:
1. Detects new files in the GCS bucket using Apache Airflow
2. Cleans and transforms the data using PySpark
3. Loads the processed data into BigQuery for analytics

By automating the transform and load steps, the pipeline ensures:
- Consistent and clean data
- Reduced manual work and human errors
- Scalable processing for large datasets

## Problem Statement

Handling daily sales data manually is time-consuming, error-prone, and hard to scale. Traditional manual processes make it difficult for businesses to:
- Quickly analyze sales performance
- Detect trends in customer behavior
- Make timely, data-driven decisions
This pipeline solves this problem by automating the transformation and loading of sales data, enabling faster, accurate insights.

## Architecture Diagram
![Architecture Diagram](https://drive.google.com/file/d/1cQ5-E8rrpW0xhGDZZmoXJtiW-TlEgWLj/view?usp=drive_link)

- GCS Bucket: Stores raw sales data (sales_data/) and PySpark code (spark_job/)
- Airflow DAG: Orchestrates the workflow and triggers the Spark job
- PySpark Job: Handles data cleaning, column renaming, type conversion, and computation checks
- BigQuery: Stores transformed, analytics-ready sales data

## Technologies Used

- Google Cloud Storage (GCS) – Stores raw files and PySpark code
- Apache Airflow (Composer) – Workflow orchestration
- PySpark – Data transformation and cleaning
- BigQuery – Data storage and analytics
- GitHub Actions – CI/CD pipeline for automatic deployment

## How the Pipeline Helps

- Automates the data workflow, reducing manual effort
- Ensures consistent and validated sales data
- Scales efficiently as daily sales data grows
- Provides a foundation for analytics and reporting
