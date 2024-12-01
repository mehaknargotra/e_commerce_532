from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql import DataFrame
from dotenv import load_dotenv
import os

load_dotenv()
import logging

# Set up logging configuration
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)



# Initialize Spark Session with appropriate configurations for Ecommerce Data Analysis
spark = SparkSession.builder \
    .appName("Ecommerce Data Analysis") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
            
kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
kafka_topic = os.getenv('KAFKA_TOPIC', 'ecommerce_customers')
# Read data from 'ecommerce_customers' topic
customerSchema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("location", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("account_created", StringType(), True),
    StructField("last_login", TimestampType(), True) 
])
customerDF = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
              .option("subscribe", kafka_topic)
              .option("startingOffsets", "earliest")  # Start from the earliest records
              .load()
              .selectExpr("CAST(value AS STRING)")
              .select(from_json("value", customerSchema).alias("data"))
              .select("data.*")
              .withWatermark("last_login", "2 hours") 
             )

query = customerDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for any of the streams to finish
spark.streams.awaitAnyTermination()