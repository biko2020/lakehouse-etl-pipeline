# 🏪 Retail Intelligence Lakehouse
### End-to-End Databricks ETL Pipeline · Medallion Architecture

> This project demonstrates my ability to design scalable, cost-efficient data pipelines on Databricks. I help businesses reduce cloud spend, ensure data reliability, and deliver analytics-ready datasets for BI and decision-making.

---

## 📌 Overview

A production-grade ETL pipeline built on the **Databricks Lakehouse** platform, following the **Medallion Architecture** (Bronze → Silver → Gold). It transforms fragmented retail data — Orders, Customers, and Products — into high-performance, analytics-ready Delta tables.

This pipeline demonstrates real-world data engineering expertise across three key pillars:

| Pillar | Implementation |
|---|---|
| **Cost Optimization** | Batch ingestion with `spark.read` optimized for Databricks Free Edition |
| **Data Integrity** | Schema enforcement, ACID transactions, and idempotent upserts with Delta Lake |
| **Performance Engineering** | Query acceleration via Z-Ordering and partitioning strategies |

---

## 🏗️ Architecture

```
┌────────────────────────┐      ┌────────────────────────┐      ┌────────────────────────┐
│      BRONZE LAYER      │      │      SILVER LAYER      │      │       GOLD LAYER       │
│   (Raw Ingestion)      │      │  (Cleanse & Model)     │      │   (Business Logic)     │
├────────────────────────┤      ├────────────────────────┤      ├────────────────────────┤
│ • Ingest CSV files     │─────▶│ • Deduplication        │─────▶│ • Sales Aggregations   │
│ • All-String Schema    │      │ • Type Casting         │      │ • Customer Lifetime Val│
│ • Metadata Injection   │      │ • Join Dim/Fact Tables │      │ • SQL Analytics Ready  │
└────────────────────────┘      └────────────────────────┘      └────────────────────────┘
```

![Architecture](docs/architecture.mermaid)

---

## 📂 Project Structure

```
lakehouse-etl-pipeline/
│
├── data/
│   └── raw/                      # Source CSVs (flat structure)
│       ├── orders.csv
│       ├── customers.csv
│       └── products.csv
│
├── notebooks/                    # Entry points for Databricks Workflows
│   ├── 01_bronze_ingest.py       # Batch ingestion → Bronze Delta tables
│   ├── 02_silver_transform.py    # Cleansing, dedup & MERGE → Silver
│   └── 03_gold_analytics.py      # Aggregations & BI-ready tables → Gold
│
├── src/                          # Core PySpark logic (reusable modules)
│   ├── bronze_pipeline.py        # Batch spark.read + Delta write
│   ├── silver_pipeline.py        # MERGE (upsert) logic & data quality checks
│   └── gold_pipeline.py          # Window functions & Spark SQL transformations
│
├── config/
│   └── pipeline_config.py        # Unity Catalog paths (CATALOG / SCHEMA / VOLUME)
│
├── docs/
│   ├── dashboard/                # Screenshots of pipeline runs & BI visuals
│   └── architecture.mermaid      # Architecture diagram
│
├── requirements.txt
└── README.md
```

---

## ⚙️ Technical Highlights

### 💰 Cost-Efficient Ingestion — Bronze Layer
Batch `spark.read` ingestion on Databricks Free Edition Serverless. All columns ingested as `StringType` — type casting is handled exclusively in the Silver layer following correct Medallion Architecture principles.

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

**1. Sign up for Databricks Free Edition**
> [databricks.com/try-databricks](https://www.databricks.com/try-databricks) — no credit card required.

**2. Import the repo**
> Workspace → Repos → Add Repo → paste your GitHub URL.

**3. Create Unity Catalog structure**
> Catalog Explorer → Create: `retail_lakehouse` catalog → `etl` schema → `storage` volume.

**4. Upload raw CSV files**
> Upload `orders.csv`, `customers.csv`, `products.csv` to:
> `/Volumes/retail_lakehouse/etl/storage/raw/`

**5. Configure paths**
> Update `config/pipeline_config.py` — set `CATALOG`, `SCHEMA`, and `VOLUME` to match your workspace.

**6. Run the pipeline sequentially**
```
01_bronze_ingest.py  →  02_silver_transform.py  →  03_gold_analytics.py
```

---

## 📊 Sample Insights

| Insight | Description |
|---|---|
| 🏆 **Top Products by Revenue** | Monthly performance ranking across all product categories |
| ⚠️ **High Churn Risk Customers** | Detects customers inactive for more than 90 days |
| 📈 **Daily Sales Trend (7-Day Avg)** | Rolling average time-series for executive reporting |

---

## 🖥️ Pipeline Screenshots

### All 9 Delta Tables — Bronze / Silver / Gold
![All 9 tables](docs/dashboard/showing%20all%209%20tables%20Bronze-Silver-Gold.png)

### Notebook 01 — Bronze Ingestion (row counts)
![Bronze run](docs/dashboard/successful%20run%20with%20row%20counts.png)

### Notebook 02 — Silver Transformation (MERGE complete)
![Silver run](docs/dashboard/successful%20run%20with%20MERGE%20complete.png)

### Notebook 03 — Gold Query Results
![Gold results](docs/dashboard/Gold%20table%20query%20results.png)

---

## 📊 BI Dashboard — Power BI

### Top Products by Revenue
![Top Products](docs/dashboard/Top%20Products%20by%20Revenue.png)

### High Churn Risk Customers
![Churn Risk](docs/dashboard/High%20Churn%20Risk%20Customers.png)

### Daily Sales Trend (7-Day Avg)
![Daily Trend](docs/dashboard/Daily%20Sales%20Trend%20(7-Day%20Avg).png)

### Full Dashboard
![Full Dashboard](docs/dashboard/all%20visuals%20together.png)

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