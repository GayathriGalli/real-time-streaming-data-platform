# ⚡ Real-Time Streaming Data Engineering Platform

A production-style **real-time data engineering platform** designed to ingest, process, validate, transform, and analyze streaming transaction data using **Apache Kafka, Apache Spark Structured Streaming, Delta Lake, Databricks, Apache Airflow, and Medallion Architecture**.

This project demonstrates how modern data engineering systems process continuously arriving data while maintaining **data quality, reliability, scalability, fault tolerance, and observability**.

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
     │  Raw Events  │
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
     │  Analytics   │
     └──────┬───────┘
            │
            ▼
      Analytics / BI

Apache Airflow → Pipeline Orchestration
Delta Lake     → Reliable Lakehouse Storage
Monitoring     → Pipeline & Data Quality Observability
GitHub Actions → Automated CI Testing
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
- Automated data quality validation
- Gold-layer business aggregations
- Apache Airflow pipeline orchestration
- Pipeline monitoring and observability
- Automated testing with Pytest
- Continuous Integration using GitHub Actions
- Docker-based local Kafka environment
- Centralized platform configuration

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
| Architecture | Medallion Architecture |
| Data Quality | Python/PySpark Validation Rules |
| Infrastructure | Docker, Docker Compose |
| Testing | Pytest |
| CI/CD | GitHub Actions |
| Version Control | Git, GitHub |

---

## 🥉 Bronze Layer

The Bronze layer represents raw streaming events received from Kafka.

Responsibilities include:

- Preserve original event payloads
- Capture ingestion metadata
- Maintain streaming checkpoints
- Support replay and troubleshooting
- Provide the raw foundation for downstream processing

---

## 🥈 Silver Layer

The Silver layer transforms raw streaming data into validated and analytics-ready records.

Processing includes:

- Schema validation
- Data type enforcement
- Null-value validation
- Timestamp standardization
- Duplicate detection and removal
- Invalid-record filtering
- Derived column creation
- Business-rule validation

The Silver layer helps ensure that downstream analytics operate on clean and reliable data.

---

## 🥇 Gold Layer

The Gold layer produces business-oriented aggregations from validated streaming data.

Example analytical metrics include:

- Revenue by product category
- Transactions per processing window
- Average order value
- Payment-method distribution
- Customer transaction activity
- Regional sales performance
- Top-performing product categories

These datasets can be consumed by **BI tools, dashboards, analytical applications, or downstream data products**.

---

## 📂 Project Structure

```text
real-time-streaming-data-platform/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── airflow/
│   └── dags/
│
├── config/
│
├── monitoring/
│
├── producer/
│
├── quality/
│   └── data_quality_checks.py
│
├── streaming/
│
├── tests/
│   └── test_data_quality.py
│
├── .gitignore
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 📊 Streaming Event Example

The platform uses synthetic transaction events to simulate continuously arriving business data.

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

The platform uses **Spark Structured Streaming** to process continuously arriving Kafka events.

The streaming pipeline demonstrates concepts including:

- Event-driven data processing
- Streaming ingestion
- Explicit schema enforcement
- Incremental transformations
- Deduplication
- Checkpoint management
- Fault recovery
- Data validation
- Medallion architecture processing

The architecture separates raw ingestion, data transformation, and analytical processing into independent layers to improve maintainability and reliability.

---

## ✅ Data Quality Framework

A dedicated data quality module validates transaction records before they are used by downstream processing.

Example validation rules include:

```text
transaction_id IS NOT NULL
customer_id IS NOT NULL
quantity > 0
unit_price >= 0
event_timestamp IS NOT NULL
```

The framework is designed to identify invalid records and calculate validation metrics that can be monitored or tested automatically.

Automated tests are maintained under:

```text
tests/test_data_quality.py
```

---

## 🔁 Pipeline Orchestration

**Apache Airflow** is used to represent orchestration of the streaming data platform.

The orchestration layer demonstrates how production data engineering workflows can coordinate:

```text
Infrastructure
      ↓
Producer
      ↓
Bronze Processing
      ↓
Silver Processing
      ↓
Gold Processing
      ↓
Data Quality Validation
      ↓
Monitoring
```

This separation keeps orchestration logic independent from transformation and validation logic.

---

## 📈 Monitoring & Observability

The project includes a dedicated monitoring component for tracking pipeline execution and data quality behavior.

Observability is an important part of production data engineering because streaming systems need visibility into:

- Processing failures
- Invalid records
- Pipeline execution
- Data quality metrics
- Streaming reliability
- Operational troubleshooting

---

## 🧪 Automated Testing

Data quality behavior is validated using **Pytest**.

Tests verify the expected behavior of validation logic and help prevent regressions when pipeline code changes.

Run tests locally using:

```bash
python -m pytest tests/ -v
```

---

## 🔄 Continuous Integration

The repository includes a **GitHub Actions CI workflow** located at:

```text
.github/workflows/ci.yml
```

The workflow automatically runs when code is pushed to the `main` branch or when a pull request targets `main`.

The CI pipeline:

1. Checks out the repository
2. Configures Python
3. Configures Java for Spark
4. Installs project dependencies
5. Configures the repository import path
6. Executes automated Pytest data-quality tests

This ensures that changes are automatically validated before being integrated into the project.

### CI Status

✅ **GitHub Actions workflow successfully passing**

---

## 🐳 Local Kafka Environment

The project includes:

```text
docker-compose.yml
```

to support local Kafka infrastructure for development and experimentation.

Docker helps provide a repeatable local environment without requiring Kafka components to be configured manually on the host machine.

---

## ▶️ Running the Project

### 1. Clone the repository

```bash
git clone https://github.com/GayathriGalli/real-time-streaming-data-platform.git
cd real-time-streaming-data-platform
```

### 2. Create a Python virtual environment

```bash
python -m venv .venv
```

Activate it on Windows:

```bash
.venv\Scripts\activate
```

On macOS/Linux:

```bash
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Start the local Kafka environment

```bash
docker compose up -d
```

### 5. Run automated tests

```bash
python -m pytest tests/ -v
```

---

## 🧠 Engineering Concepts Demonstrated

This project focuses on practical data engineering concepts including:

**Real-Time Data Engineering • Streaming ETL/ELT • Apache Kafka • Spark Structured Streaming • PySpark • Distributed Processing • Event-Driven Architecture • Medallion Architecture • Lakehouse Architecture • Delta Lake • Data Quality • Schema Enforcement • Deduplication • Checkpointing • Fault Tolerance • Pipeline Orchestration • Monitoring • Automated Testing • CI/CD • Docker**

---

## 🎯 Why This Project

Modern data platforms increasingly need to process data as it arrives rather than relying only on scheduled batch pipelines.

This project was created to demonstrate how a real-time data engineering architecture can combine:

**Kafka → Spark Structured Streaming → Medallion Architecture → Data Quality → Airflow → Monitoring → CI/CD**

into a modular engineering workflow.

It is intended as a hands-on portfolio implementation of concepts commonly used in modern cloud and lakehouse data platforms.

---

## 🗺️ Implementation Status

- ✅ Synthetic transaction event generator
- ✅ Kafka streaming infrastructure configuration
- ✅ Bronze streaming processing
- ✅ Silver transformation logic
- ✅ Data validation and deduplication
- ✅ Gold aggregation logic
- ✅ Data quality framework
- ✅ Airflow orchestration
- ✅ Monitoring and observability components
- ✅ Automated Pytest validation
- ✅ GitHub Actions CI pipeline
- ✅ Docker-based local infrastructure
- ✅ Centralized configuration
- ⏳ Additional Databricks/cloud execution validation
- ⏳ Expanded integration testing
- ⏳ BI/dashboard integration

---

## 🔮 Future Enhancements

Future improvements can include:

- Cloud deployment using AWS, Azure, or GCP
- Managed Kafka integration
- Databricks Jobs and Workflows
- Unity Catalog governance
- Schema Registry integration
- Dead-letter queue handling
- Great Expectations or advanced data quality tooling
- Prometheus/Grafana monitoring
- Infrastructure as Code using Terraform
- BI dashboard integration
- Performance and load testing
- End-to-end integration tests

---

## 👩‍💻 Author

**Gayathri Galli**

**Data Engineer | AI/ML Engineer | Generative AI**

Python • SQL • PySpark • Databricks • Kafka • Spark • Cloud Data Engineering

---

## 📌 Project Status

🚧 **Active Portfolio Development**

The core repository structure, streaming components, data quality framework, orchestration, monitoring, automated tests, Docker configuration, and CI pipeline have been implemented.

Additional cloud and Databricks integration can be added as the platform evolves.
