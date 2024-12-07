# E-Commerce


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
