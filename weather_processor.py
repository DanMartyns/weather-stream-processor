import os
import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

logging.basicConfig(
    level=logging.INFO, # Set the logging level
    format="%(asctime)s - %(levelname)s - %(message)s", # Format with timestamps
    handlers=[
        logging.StreamHandler(sys.stdout) # Log to stdout to be captured by Docker
    ]
)

log = logging.getLogger(__name__)

time.sleep(5)

# Constants
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")

# Define schema for the Kafka stream data
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("total_precipitation", DoubleType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("WeatherDataProcessor") \
    .getOrCreate()

# Read data from Kafka
weather_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON data from Kafka
weather_data = weather_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

log.info(weather_data)

# Aggregate precipitation over time (you can adjust windowing if needed)
precipitation_summary = weather_data \
    .groupBy("timestamp") \
    .agg(spark_sum("total_precipitation").alias("hourly_precipitation"))

log.info(precipitation_summary)

# Write the processed data to the console (can also be written to a database or file system)
query = precipitation_summary \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()