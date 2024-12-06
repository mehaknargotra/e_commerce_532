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


