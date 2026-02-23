# 🏎️ Pulse Stream: Enterprise IoT Telemetry Pipeline

![Python](https://img.shields.io/badge/Python-3.9-blue?logo=python&logoColor=white) ![Google Cloud](https://img.shields.io/badge/Google_Cloud-GCP-red?logo=googlecloud&logoColor=white) ![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-Streaming-black?logo=apachekafka&logoColor=white) ![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-Orchestration-green?logo=apacheairflow&logoColor=white) ![Apache Spark](https://img.shields.io/badge/Apache_Spark-Processing-orange?logo=apachespark&logoColor=white) ![Docker](https://img.shields.io/badge/Docker-Containerization-blue?logo=docker&logoColor=white)

## 📖 Project Overview
Pulse Stream is an end-to-end, fault-tolerant **Lambda Architecture** data pipeline. It simulates a live athletic race, generating high-frequency telemetry data (GPS, Heart Rate, Speed) from IoT sensors, and processes that data for both real-time operational monitoring and historical batch analytics.

This project demonstrates enterprise-grade data engineering practices, including decoupled orchestration (Airflow + Docker), distributed compute (PySpark), cloud object storage integration (GCP), and strict data quality enforcement via a Dead Letter Queue (DLQ).

---

## 🏗️ Architecture & Data Flow

The pipeline is split into three distinct layers to handle massive data volume efficiently:

### 1. The Speed Layer (Real-Time Ingestion)
* A Python producer (`generator.py`) generates continuous JSON payloads simulating live athlete vitals and GPS coordinates.
* Events are published to **Confluent Apache Kafka**, which buffers the high-throughput stream.
* A consumer script (`gcs_consumer.py`) constantly drains the Kafka topic and lands the raw, unstructured JSON payloads into a **Google Cloud Storage (GCS)** data lake (Bronze Layer).

### 2. The Batch Layer (Orchestration & Processing)
* **Apache Airflow** (running in a local Docker container) acts as the orchestrator.
* On a set schedule, Airflow reaches across the Docker daemon to trigger a distributed **PySpark** job (`process_data.py`) via `spark-submit`.
* **Medallion Architecture & DLQ:** PySpark reads the raw Bronze data, enforces a strict schema, and performs transformations.
  * *Clean records* are converted to columnar Parquet files and written to the Silver Layer.
  * *Malformed records* are caught and quarantined in a **Dead Letter Queue (DLQ)**, ensuring 99.9% data accuracy and zero pipeline failures.

### 3. The Serving Layer (Data Warehouse & Dashboards)
* Clean Silver data is loaded into **Google BigQuery**, where it is aggregated into a Gold Layer for analytics.
* **Looker Studio** connects to BigQuery for sub-second, historical geospatial analytics.
* A custom **Streamlit Operations App** (`app.py`) acts as a live control plane, monitoring pipeline latency and data quality.

```mermaid
graph TD
    subgraph Sources
        IoT[("📡 IoT Sensors<br>(generator.py)")]
    end

    subgraph Speed_Layer [Real-Time Ingestion]
        Kafka[("Apache Kafka<br>(Confluent Cloud)")]
    end

    subgraph Data_Lake [Google Cloud Storage]
        Bronze[("🟤 Bronze Layer<br>(Raw JSON)")]
        Silver[("⚪ Silver Layer<br>(Clean Parquet)")]
        DLQ[("🛑 Dead Letter Queue<br>(Bad Records)")]
    end

    subgraph Orchestration_Compute [Local Docker Environment]
        Airflow[("⏱️ Apache Airflow<br>(Orchestrator)")]
        Spark[("⚡ PySpark Container<br>(process_data.py)")]
    end

    subgraph Consumption_Layer [Dashboards]
        Looker[("📊 Looker Studio<br>(Live Dashboard)")]
        Streamlit[("🛠️ Streamlit App<br>(Ops Control Plane)")]
    end

    IoT -->|High-throughput| Kafka
    Kafka -->|gcs_consumer.py| Bronze
    Airflow -.->|spark-submit| Spark
    Spark -->|Reads| Bronze
    Spark -->|Validates & Transforms| Silver
    Spark -->|Quarantines| DLQ
    Silver -->|Load BigQuery| Looker
    Kafka -->|Live Analytics| Streamlit
    Silver -->|Historical Trends| Streamlit

    style IoT fill:#f9f,stroke:#333,stroke-width:2px
    style Kafka fill:#ff9,stroke:#333,stroke-width:2px
    style Data_Lake fill:#e1f5fe,stroke:#333,stroke-width:2px
    style Orchestration_Compute fill:#fff3e0,stroke:#333,stroke-width:2px
    style Consumption_Layer fill:#e8f5e9,stroke:#333,stroke-width:2px
