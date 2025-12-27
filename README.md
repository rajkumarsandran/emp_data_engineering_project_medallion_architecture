# Databricks Lakehouse -- Medallion Architecture Implementation

## Overview

This project demonstrates an end-to-end Lakehouse implementation using
Apache Spark, Delta Lake, and Databricks, following the Medallion
Architecture (Bronze, Silver, Gold). The solution focuses on incremental
ingestion, historical tracking (SCD Type 2), and analytics-ready data
models.

## Architecture

Source → Bronze → Silver → Gold

## Technologies Used

-   Apache Spark (PySpark)
-   Delta Lake
-   Spark SQL
-   Databricks
-   Python

## Bronze Layer -- Raw Ingestion

-   Schema-enforced Delta tables
-   Incremental ingestion with processed file tracking
-   Metadata capture for auditability
-   Idempotent design using control tables

## Silver Layer -- Cleansed & Historical Data

-   Surrogate keys and hash-based change detection
-   Slowly Changing Dimension (SCD Type 2)
-   Window functions for deduplication
-   Delta Lake MERGE for updates and inserts

## Gold Layer -- Curated & Analytics Ready

-   Dimension tables with incremental upserts
-   Fact tables with aggregated metrics
-   Optimized for analytical consumption

## Key Data Engineering Concepts

-   Medallion Architecture
-   Delta Lake ACID transactions
-   Incremental & idempotent pipelines
-   SCD Type 2
-   Hash-based change detection
-   Spark SQL & PySpark

## Author

Rajkumar Rajendran Data Engineer \| Azure Databricks \| PySpark \| SQL
