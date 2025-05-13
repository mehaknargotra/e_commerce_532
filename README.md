# 🛒 Real-Time E-commerce Analytics System

This project simulates and analyzes real-time user interactions, purchases, and product views in an e-commerce platform using streaming data tools.

## 🚀 Features
- Kafka-based event ingestion pipeline (10K+ TPS)
- Stream processing using PySpark
- Real-time dashboards with Grafana
- Elasticsearch integration for search and analytics
- Performance tuning for low-latency, high-throughput data processing

## 🔧 Tech Stack
Kafka • PySpark • Python • Grafana • Elasticsearch • Docker

## 📈 Use Case
Ideal for simulating and monitoring e-commerce behavior in real time — such as customer views, cart actions, and purchases — to generate meaningful analytics for operational dashboards.

## 📁 Folder Structure
- `producers/` – Kafka event producers (e.g., clickstream data)
- `spark_jobs/` – Spark streaming scripts for processing events
- `dashboards/` – Grafana dashboards or configuration templates
- `requirements.txt` – Python dependencies

## ⚙️ Setup Instructions 
## Setup PySpark

Packages to install

```bash
pip install -r requirements.txt
```

To generate data and stream it through kafka run the following path

```bash

python3 main.py
```

Run the following command to build spark session 
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.16.1 spark_processing.py
```

## Setup Grafana

Run the command to download Grafana:
```bash
curl -O https://dl.grafana.com/enterprise/release/grafana-enterprise-11.3.2.darwin-amd64.tar.gz
```

Extract the downloaded file:
```bash
tar -zxvf grafana-enterprise-11.3.2.darwin-amd64.tar.gz
```

Running Grafana:
```bash
./bin/grafana server
```

## Configuring Elasticsearch in Grafana

    1. Log in to Grafana
    2. Go to Configuration → Data Sources → Add data source
    3. Select Elasticsearch

    # Configure Connection
    1. Name: Enter a name e.g., `Elasticsearch`
    2. URL: Elasticsearch URL e.g., `http://localhost:9200`
    3. Index Name: Enter index e.g., `customer_index`
    4. Time Field Name: Specify time field e.g., `@timestamp`
    5. Click *Save & Test* to validate the connection
