# 🏎️ Pulse Stream: Real-Time IoT Telemetry Pipeline

![Python](https://img.shields.io/badge/Python-3.9-blue) ![GCP](https://img.shields.io/badge/Google_Cloud-Platform-red) ![Kafka](https://img.shields.io/badge/Apache_Kafka-Streaming-black) ![Airflow](https://img.shields.io/badge/Apache_Airflow-Orchestration-green) ![PySpark](https://img.shields.io/badge/Apache_Spark-Processing-orange) ![BigQuery](https://img.shields.io/badge/BigQuery-Data_Warehousing-blue) 

## 📖 Project Overview
Pulse Stream is an end-to-end, fault-tolerant **Lambda Architecture** data pipeline designed to ingest, process, and visualize high-frequency telemetry data (GPS, Heart Rate, Speed) from IoT sensors.

Designed to simulate a live athletic race, this system handles massive data ingestion via **Apache Kafka**, orchestrates PySpark ELT jobs via **Apache Airflow**, enforces schema validation using a **Dead Letter Queue (DLQ)**, and serves analytics via **BigQuery**, **Looker Studio**, and a custom **Streamlit Operations Portal**.

### 🏗️ Architecture

```mermaid
graph TD
    subgraph Sources
        IoT[("📡 IoT Sensors<br>(Python Simulator)")]
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
        Spark[("⚡ PySpark Container<br>(Data Processing)")]
    end

    subgraph Warehouse_Layer [Google BigQuery]
        Gold[("🟡 Gold Tables<br>(Aggregated Analytics)")]
    end

    subgraph Consumption_Layer [Dashboards]
        Looker[("📊 Looker Studio<br>(Live Dashboard)")]
        Streamlit[("🛠️ Streamlit App<br>(Ops Control Plane)")]
    end

    IoT -->|High-throughput| Kafka
    Kafka -->|Ingestion Script| Bronze
    Airflow -.->|spark-submit| Spark
    Spark -->|Reads| Bronze
    Spark -->|Validates & Transforms| Silver
    Spark -->|Quarantines| DLQ
    Silver -->|Load| Gold
    Gold -->|Sub-second Queries| Looker
    Kafka -->|Live Consumer| Streamlit
    Gold -->|Historical Trends| Streamlit

    style IoT fill:#f9f,stroke:#333,stroke-width:2px
    style Kafka fill:#ff9,stroke:#333,stroke-width:2px
    style Data_Lake fill:#e1f5fe,stroke:#333,stroke-width:2px
    style Orchestration_Compute fill:#fff3e0,stroke:#333,stroke-width:2px
    style Warehouse_Layer fill:#e8eaf6,stroke:#333,stroke-width:2px
    style Consumption_Layer fill:#e8f5e9,stroke:#333,stroke-width:2px
