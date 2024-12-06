from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import udf
from textblob import TextBlob
from pyspark.sql import DataFrame
from dotenv import load_dotenv
import os

load_dotenv()
import logging

# Set up logging configuration
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)



def analyze_sentiment(text):
    if text:
        analysis = TextBlob(text)
        if analysis.sentiment.polarity > 0:
            return "positive"
        elif analysis.sentiment.polarity < 0:
            return "negative"
        else:
            return "neutral"
    return "neutral"

# Register UDF
sentiment_udf = udf(analyze_sentiment, StringType())

# Initialize Spark Session with appropriate configurations for Ecommerce Data Analysis
spark = SparkSession.builder \
    .appName("Ecommerce Data Pipeline") \
    .config("spark.es.nodes", "localhost") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
            
kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
# kafka_topic = os.getenv('KAFKA_TOPIC', 'ecommerce_customers')
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
              .option("subscribe", 'customer_data')
              .option("startingOffsets", "earliest")  # Start from the earliest records
              .load()
              .selectExpr("CAST(value AS STRING)")
              .select(from_json("value", customerSchema).alias("data"))
              .select("data.*")
              .withColumn("processingTime", current_timestamp()) 
              .withWatermark("last_login", "2 hours") 
             )
customerDF = customerDF.withColumn("@timestamp", col("processingTime"))

# Read data from 'ecommerce_products' topic
productSchema = StructType([
    StructField("product_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("supplier", StringType(), True),
    StructField("rating", DoubleType(), True)
])
productDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "product_data") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", productSchema).alias("data")) \
    .select("data.*") \
    .withColumn("processingTime", current_timestamp())  # Add processing timestamp

productDF = productDF.withColumn("@timestamp", col("processingTime"))
productDF = productDF.withWatermark("processingTime", "2 hours")


# Read data from 'ecommerce_transactions' topic
transactionSchema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("date_time", TimestampType(), True),  
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True)
])
transactionDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "transaction_data") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", transactionSchema).alias("data")) \
    .select("data.*")

transactionDF = transactionDF.withColumn("processingTime", current_timestamp())
transactionDF = transactionDF.withColumn("@timestamp", col("processingTime"))
transactionDF = transactionDF.withWatermark("processingTime", "2 hours")


# Read data from 'ecommerce_product_views' topic
productViewSchema = StructType([
    StructField("view_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),  
    StructField("view_duration", IntegerType(), True)
])
productViewDF = (spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                 .option("subscribe", "product_view_data")
                 .option("startingOffsets", "earliest")
                 .load()
                 .selectExpr("CAST(value AS STRING)")
                 .select(from_json("value", productViewSchema).alias("data"))
                 .select("data.*")
                 .withColumn("timestamp", col("timestamp").cast("timestamp"))
                 .withWatermark("timestamp", "1 hour")
                 )
productViewDF = productViewDF.withColumn("processingTime", current_timestamp())
productViewDF = productViewDF.withColumn("@timestamp", col("processingTime"))

# productViewDF = productViewDF.withWatermark("processingTime", "2 hours")

# Read data from 'ecommerce_system_logs' topic
systemLogSchema = StructType([
    StructField("log_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),  
    StructField("level", StringType(), True),
    StructField("message", StringType(), True)
])

systemLogDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "system_log_data") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", systemLogSchema).alias("data")) \
    .select("data.*")

systemLogDF = systemLogDF.withColumn("processingTime", current_timestamp())
systemLogDF = systemLogDF.withColumn("@timestamp", col("processingTime"))
systemLogDF = systemLogDF.withWatermark("processingTime", "2 hours")

# Read data from 'ecommerce_user_interactions' topic
userInteractionSchema = StructType([
    StructField("interaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),  
    StructField("interaction_type", StringType(), True),
    StructField("details", StringType(), True)
])

userInteractionDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "user_interaction_data") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", userInteractionSchema).alias("data")) \
    .select("data.*")

userInteractionDF = userInteractionDF.withColumn("processingTime", current_timestamp())
userInteractionDF = userInteractionDF.withColumn("@timestamp", col("processingTime"))
userInteractionDF = userInteractionDF.withWatermark("processingTime", "2 hours")

reviewsDF = userInteractionDF.filter(userInteractionDF["interaction_type"] == "review")
reviewsDF = reviewsDF.withColumn("sentiment", sentiment_udf(reviewsDF["details"]))
reviewsDF = reviewsDF[['product_id', 'sentiment', '@timestamp']]

#This analysis  focus on demographics and account activity.
customerAnalysisDF = (customerDF
                      .groupBy(
                          window(col("last_login"), "1 day"),  # Windowing based on last_login
                          "gender"
                      )
                      .agg(
                          count("customer_id").alias("total_customers"),
                          max("last_login").alias("last_activity")
                      )
                     )


# Analyzing product popularity and stock status with windowing
productAnalysisDF = productDF \
    .groupBy(
        window(col("processingTime"), "1 hour"),  # Window based on processingTime
        "category"
    ) \
    .agg(
        avg("price").alias("average_price"),
        sum("stock_quantity").alias("total_stock")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("category"),
        col("average_price"),
        col("total_stock")
    )


# ordered customer activity in terms of the number of interactions they have with products, such as wishlist additions, reviews, or ratings.
customerActivityDF = (userInteractionDF
                      .groupBy("customer_id")
                      .agg(
                          count("interaction_id").alias("total_interactions"),
                          count(when(col("interaction_type") == "wishlist_addition", 1)).alias("wishlist_additions"),
                          count(when(col("interaction_type") == "review", 1)).alias("reviews"),
                          count(when(col("interaction_type") == "rating", 1)).alias("ratings")
                      ).orderBy(col("total_interactions").desc()) 
                     )
customerActivityDF = customerActivityDF.withColumn("@timestamp", current_timestamp())
top20CustomersDF = customerActivityDF.limit(20)




# low stock level alert
salesVelocityDF = transactionDF.groupBy(
    col("product_id")
).agg(
    avg("quantity").alias("average_daily_sales")
)

salesVelocityDF.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("sales_velocity") \
    .option("checkpointLocation", "/tmp/sales_velocity_checkpoint") \
    .start()

spark.sql("SELECT * FROM sales_velocity").show()
productWithThresholdDF = productDF.join(
    spark.sql("SELECT * FROM sales_velocity"), 
    "product_id",
    "left_outer"
).withColumn(
    "threshold", col("average_daily_sales") * 2  
).fillna({"threshold": 7}) 

# Filter for Low Stock
lowStockDF = productWithThresholdDF.filter(
    col("stock_quantity") < col("threshold")
).select(
    "product_id", "name", "stock_quantity", "threshold", "processingTime"
).withColumn(
    "alert", lit("Low stock level detected")
)

#number of customers retained - retention analysis
retentionDF = transactionDF.groupBy(
    col("customer_id")
).agg(
    count("transaction_id").alias("purchase_count")
).filter(
    col("purchase_count") > 1
)

# query_3 = retentionDF.writeStream \
#     .outputMode("update") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()

query_1 = lowStockDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# query_2 = customerAnalysisDF.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start()

def writeToElasticsearch(df, index_name):
    """
    Function to write Spark DataFrame to Elasticsearch.
    Args:
        df (DataFrame): Spark DataFrame to be written.
        index_name (str): Elasticsearch index name.
    """
    def write_and_log(batch_df, batch_id):
        """
        Function to write each batch of data to Elasticsearch and log the process.
        Args:
            batch_df (DataFrame): Batch DataFrame from Spark.
            batch_id (int): Identifier for the batch.
        """
        logger.info(f"Attempting to write batch {batch_id} to Elasticsearch index {index_name}.")
        try:
            if not batch_df.isEmpty():
                logger.info(f"Batch {batch_id} has data. Writing to Elasticsearch.")
                batch_df.write \
                    .format("org.elasticsearch.spark.sql") \
                    .option("checkpointLocation", f"/opt/bitnami/spark/checkpoint/{index_name}/{batch_id}") \
                    .option("es.resource", f"{index_name}/doc") \
                    .option("es.nodes", "elasticsearch") \
                    .option("es.port", "9200") \
                    .option("es.nodes.wan.only", "true") \
                    .save()
                logger.info(f"Batch {batch_id} written successfully.")
            else:
                logger.info(f"Batch {batch_id} is empty. Skipping write.")
        except Exception as e:
            logger.error(f"Error writing batch {batch_id} to Elasticsearch: {e}")

    return df.writeStream \
             .outputMode("append") \
             .foreachBatch(write_and_log) \
             .start()

writeToElasticsearch(customerAnalysisDF, "customer_analysis")
writeToElasticsearch(productAnalysisDF, "product_analysis")
writeToElasticsearch(reviewsDF, "sentiment_analysis")
writeToElasticsearch(top20CustomersDF, "customer_activity_analysis")
writeToElasticsearch(lowStockDF, "low_stock_alerts")
writeToElasticsearch(retentionDF, "customer_Retention_analysis")

# Wait for any of the streams to finish
spark.streams.awaitAnyTermination()