# ⚡ Real-Time Streaming Data Engineering Platform

A production-style **real-time data engineering platform** designed to ingest, process, validate, transform, and analyze streaming transaction data using **Apache Kafka, Apache Spark Structured Streaming, Delta Lake, Databricks, Apache Airflow, and Medallion Architecture**.

This project demonstrates how modern data engineering systems process continuously arriving data while maintaining **data quality, reliability, scalability, and observability**.

---

## 🏗️ Architecture

```text
Synthetic Transaction Generator
            │
            ▼
      Apache Kafka
            │
            ▼
 Spark Structured Streaming
            │
            ▼
     ┌──────────────┐
     │ BRONZE LAYER │
     │ Raw Events   │
     └──────┬───────┘
            │
            ▼
     ┌──────────────┐
     │ SILVER LAYER │
     │ Cleaned Data │
     │ Validation   │
     │ Deduplication│
     └──────┬───────┘
            │
            ▼
     ┌──────────────┐
     │  GOLD LAYER  │
     │ Aggregations │
     │ Analytics    │
     └──────┬───────┘
            │
            ▼
      Analytics / BI

Apache Airflow → Pipeline Orchestration
Delta Lake     → Reliable Lakehouse Storage
Monitoring     → Pipeline & Data Quality Observability
```

---

## 🚀 Key Features

- Real-time event generation using Python
- Streaming data ingestion with Apache Kafka
- Distributed stream processing using Spark Structured Streaming
- Explicit schema enforcement and validation
- Bronze, Silver, and Gold Medallion Architecture
- Data cleansing and transformation using PySpark
- Duplicate event detection and removal
- Invalid-record handling
- Streaming checkpointing and fault recovery
- Delta Lake storage and ACID transaction support
- Data quality validation
- Gold-layer business aggregations
- Apache Airflow orchestration
- SQL-based analytical queries
- Pipeline monitoring and observability
- Automated testing for transformation logic
- Docker-based local development environment

---

## 🛠️ Technology Stack

| Category | Technologies |
|---|---|
| Programming | Python, SQL |
| Streaming | Apache Kafka |
| Processing | Apache Spark, PySpark, Spark Structured Streaming |
| Lakehouse | Delta Lake |
| Platform | Databricks |
| Orchestration | Apache Airflow |
| Data Architecture | Medallion Architecture |
| Data Quality | PySpark validation rules |
| Infrastructure | Docker, Docker Compose |
| Testing | Pytest |
| Version Control | Git, GitHub |

---

## 🥉 Bronze Layer

The Bronze layer stores raw streaming events received from Kafka.

Responsibilities:

- Preserve original event payloads
- Add ingestion metadata
- Maintain streaming checkpoints
- Support replay and troubleshooting
- Persist raw data in Delta format

---

## 🥈 Silver Layer

The Silver layer converts raw events into validated and analytics-ready records.

Processing includes:

- Schema validation
- Data type enforcement
- Null-value validation
- Timestamp standardization
- Duplicate removal
- Invalid-record filtering
- Derived column creation
- Business-rule validation

---

## 🥇 Gold Layer

The Gold layer contains business-level aggregations optimized for analytics.

Example metrics:

- Revenue by product category
- Transactions per minute
- Average order value
- Payment-method distribution
- Customer transaction activity
- Regional sales performance
- Top-selling products

---

## 📂 Project Structure

```text
real-time-streaming-data-platform/
│
├── producer/
│   └── transaction_producer.py
│
├── streaming/
│   ├── bronze_ingestion.py
│   ├── silver_transformation.py
│   └── gold_aggregation.py
│
├── schemas/
│   └── transaction_schema.py
│
├── data_quality/
│   └── quality_checks.py
│
├── airflow/
│   └── dags/
│       └── streaming_pipeline_dag.py
│
├── sql/
│   └── analytics_queries.sql
│
├── tests/
│   └── test_transformations.py
│
├── config/
│   └── config.yaml
│
├── docs/
│   └── architecture.md
│
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 📊 Streaming Event Example

```json
{
  "transaction_id": "TXN-100001",
  "customer_id": "CUST-2045",
  "product_id": "PROD-105",
  "category": "Electronics",
  "quantity": 2,
  "unit_price": 249.99,
  "payment_method": "Credit Card",
  "event_timestamp": "2026-08-31T14:30:00Z",
  "country": "United States"
}
```

---

## 🔄 Streaming Processing

The platform uses **Spark Structured Streaming** to continuously consume Kafka events.

The pipeline is designed to demonstrate:

- Event-time processing
- Checkpoint management
- Fault-tolerant streaming
- Schema enforcement
- Deduplication
- Stateful processing concepts
- Incremental transformations

---

## ✅ Data Quality

Quality rules validate incoming transactions before they reach analytics layers.

Example checks include:

```text
transaction_id IS NOT NULL
customer_id IS NOT NULL
quantity > 0
unit_price >= 0
event_timestamp IS NOT NULL
```

Records that fail validation can be separated from valid records for investigation.

---

## 📈 Analytics

Gold-layer datasets are designed to support analytical queries such as:

```sql
SELECT
    category,
    COUNT(*) AS total_transactions,
    SUM(quantity * unit_price) AS total_revenue
FROM gold_transactions
GROUP BY category
ORDER BY total_revenue DESC;
```

---

## 🎯 Engineering Concepts Demonstrated

This project focuses on practical Data Engineering concepts including:

**Streaming Data Engineering • ETL/ELT • Distributed Processing • Event-Driven Architecture • Medallion Architecture • Lakehouse Architecture • Data Quality • Schema Enforcement • Deduplication • Checkpointing • Fault Tolerance • Pipeline Orchestration • Data Modeling • Monitoring • Analytics Engineering**

---

## 🗺️ Implementation Roadmap

- [ ] Build synthetic transaction event generator
- [ ] Configure Kafka streaming infrastructure
- [ ] Define transaction schema
- [ ] Implement Bronze streaming ingestion
- [ ] Implement Silver transformations
- [ ] Add deduplication and validation
- [ ] Implement Gold aggregations
- [ ] Add Delta Lake storage
- [ ] Add checkpointing
- [ ] Add data quality framework
- [ ] Create Airflow orchestration
- [ ] Add analytical SQL queries
- [ ] Add automated tests
- [ ] Add monitoring
- [ ] Add architecture documentation
- [ ] Validate Databricks execution

---

## 👩‍💻 Author

**Gayathri Galli**

Data Engineer | AI/ML Engineer | Generative AI  
Python | SQL | PySpark | Databricks | Kafka | Spark | Cloud Data Engineering

---

## 📌 Project Status

🚧 **Under active development**

The repository is being built incrementally to demonstrate an end-to-end production-style streaming data engineering architecture.
