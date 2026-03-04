# 🏪 Retail Intelligence Lakehouse
### End-to-End Databricks ETL Pipeline · Medallion Architecture

---

## 📌 Overview

A production-grade ETL pipeline built on the **Databricks Lakehouse** platform, following the **Medallion Architecture** (Bronze → Silver → Gold). It transforms fragmented retail data — Orders, Customers, and Products — into high-performance, analytics-ready Delta tables.

This pipeline demonstrates real-world data engineering expertise across three key pillars:

| Pillar | Implementation |
|---|---|
| **Cost Optimization** | Incremental ingestion via Databricks Autoloader to minimize compute costs |
| **Data Integrity** | Schema enforcement, ACID transactions, and idempotent upserts with Delta Lake |
| **Performance Engineering** | Query acceleration via Z-Ordering and partitioning strategies |

---

## 🏗️ Architecture

```
┌────────────────────────┐      ┌────────────────────────┐      ┌────────────────────────┐
│      BRONZE LAYER      │      │      SILVER LAYER      │      │       GOLD LAYER       │
│   (Raw Ingestion)      │      │  (Cleanse & Model)     │      │   (Business Logic)     │
├────────────────────────┤      ├────────────────────────┤      ├────────────────────────┤
│ • Ingest CSV/JSON      │─────▶│ • Deduplication        │─────▶│ • Sales Aggregations   │
│ • Schema Evolution     │      │ • Type Casting         │      │ • Customer Lifetime Val│
│ • Metadata Injection   │      │ • Join Dim/Fact Tables │      │ • SQL Analytics Ready  │
└────────────────────────┘      └────────────────────────┘      └────────────────────────┘
```

---

## 📂 Project Structure

```
lakehouse-etl-pipeline/
│
├── data/                         # Local simulation of Cloud Storage (S3/ADLS)
│   ├── raw/                      # Source CSVs (Orders, Customers, Payments)
│   └── delta/                    # Delta Lake storage (Bronze / Silver / Gold)
│
├── notebooks/                    # Entry points for Databricks Workflows
│   ├── 01_bronze_ingest.py       # Autoloader ingestion
│   ├── 02_silver_transform.py    # Cleansing & modeling
│   └── 03_gold_analytics.py      # Aggregations & BI-ready tables
│
├── src/                          # Core PySpark logic (reusable modules)
│   ├── bronze_pipeline.py        # Schema inference & cloudFiles setup
│   ├── silver_pipeline.py        # MERGE (upsert) logic & data quality checks
│   └── gold_pipeline.py          # Spark SQL transformations
│
├── config/                       # Pipeline configuration
│   └── pipeline_config.py        # Path settings (DBFS / S3 / ADLS)
│
├── docs/                         # Architecture diagrams & data dictionary
├── requirements.txt              # Dependencies
└── README.md
```

---

## ⚙️ Technical Highlights

### 💰 Cost-Efficient Ingestion — Autoloader
Incremental file processing reduces cluster uptime and compute costs by only processing new or changed files rather than full reloads.

### 🔁 Idempotent Upserts — Silver Layer
Delta Lake `MERGE` ensures that pipeline retries never produce duplicates. The Silver layer serves as the **Single Source of Truth** for all downstream consumers.

### ⭐ Star Schema Modeling
- **Fact tables** — Sales transactions
- **Dimension tables** — Products, Customers
- Optimized layout for BI tools in the Gold layer

### ⚡ Performance Optimization
- **Z-Ordering** on `customer_id` and `order_date` for fast filtered queries
- **Partitioning** by `year_month` for large fact tables to minimize data scans

---

## 🚀 How to Run

**1. Clone to Databricks**
> Use Databricks Repos to import this repository directly into your workspace.

**2. Configure Paths**
> Update `config/pipeline_config.py` — set your `CATALOG`, `SCHEMA`, and `VOLUME` names to match what you created in Databricks Catalog Explorer.

**3. Run the Pipeline**
> Execute notebooks sequentially, or orchestrate via Databricks Workflows:
```
01_bronze_ingest.py  →  02_silver_transform.py  →  03_gold_analytics.py
```

---

## 📊 Sample Insights

| Insight | Description |
|---|---|
| 🏆 **Top 5 Products by Revenue** | Monthly performance ranking across all product categories |
| ⚠️ **Customer Churn Risk** | Detects customers inactive for more than 90 days |
| 📈 **Daily Sales Trend** | Time-series analysis for executive and stakeholder reporting |

---

## 📜 License

This project is licensed under the **MIT License**. See the `LICENSE` file for details.

---

## 👤 Author

**Brahim Ait Oufkir**  
*Big Data Engineer · Freelance Consultant*

[![Email](https://img.shields.io/badge/Email-aitoufkirbrahimab%40gmail.com-blue?style=flat-square&logo=gmail)](mailto:aitoufkirbrahimab@gmail.com)
[![GitHub](https://img.shields.io/badge/GitHub-%40biko2020-black?style=flat-square&logo=github)](https://github.com/biko2020)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-brahim--aitoufkir-0A66C2?style=flat-square&logo=linkedin)](https://linkedin.com/in/brahim-aitoufkir)

---

### 💡 Open to Freelance Projects

- Data Engineering — ETL, Lakehouse, Pipelines
- Cloud Migration & Cost Optimization
- BI Dashboard Development
- Technical Architecture Reviews
- Team Training & Mentorship