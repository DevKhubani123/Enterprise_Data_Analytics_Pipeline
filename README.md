Enterprise Data Processing Platform

Overview

The Enterprise Data Processing Platform is a scalable data engineering solution developed using Azure Databricks and PySpark following the Medallion Architecture (Bronze, Silver, and Gold layers). The platform processes customer, prospect, and watch-history datasets while supporting incremental ingestion, historical tracking, dimensional modeling, and analytics-ready data delivery.

---

Architecture

The pipeline follows a layered Medallion Architecture:

Configuration Layer

- Reference Configuration
- Reference File
- Customer Configuration

Landing Layer

- Customer Landing
- Prospect Landing
- Watch History Landing

Bronze Layer

- Customer Bronze
- Prospect Bronze
- Watch History Bronze

Silver Layer

- Customer Silver
- Prospect Silver
- Watch History Silver

Gold Layer

- Customer Gold
- Prospect Gold
- Security Gold
- Watch History Gold

---

Technology Stack

- Azure Databricks
- PySpark
- Delta Lake
- SQL
- Change Data Capture (CDC)
- Slowly Changing Dimension Type 2 (SCD Type-2)
- Medallion Architecture
- Data Modeling

---

Key Features

Incremental Processing

Implemented CDC pipelines to process only newly inserted and updated records.

Historical Data Tracking

Implemented SCD Type-2 logic to maintain historical versions of customer records.

Multi-Layer Data Architecture

Designed Bronze, Silver, and Gold layers for raw ingestion, transformation, and analytics consumption.

Data Quality Framework

Implemented validation checks and business rules to ensure data consistency and reliability.

Analytics-Ready Data Models

Built dimensional and fact tables to support reporting and analytical workloads.

---

Data Flow

Reference Config
→ Reference File
→ Customer Config
→ Landing Layer
→ Bronze Layer
→ Silver Layer
→ Gold Layer
→ Analytics Consumption

---

Project Components

Customer Domain

Processes customer onboarding, updates, and historical customer information.

Prospect Domain

Tracks prospect information and conversion-related analytics.

Watch History Domain

Processes security watch-history data for downstream analytics and reporting.

---

Outcomes

- Automated enterprise data processing workflows
- Historical customer tracking using SCD Type-2
- Incremental processing using CDC
- Analytics-ready dimensional models
- Improved data quality and governance

---

Author

Dev Khubani
