import os
import sys
import logging
import asyncio
import psycopg2
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_json, col, sum, window, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stderr)  # Log to stderr to be captured by Docker
    ],
)

log = logging.getLogger(__name__)

# Constants
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")

POSTGRES_DB = os.environ.get("POSTGRES_DB")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")


async def wait_for_kafka_connection(bootstrap_servers, topic, retries=10, delay=2):
    """
    Asynchronously tries to establish a connection to Kafka using Spark with a retry mechanism.
    """
    for attempt in range(retries):
        try:
            # Try to create a Spark session and read from Kafka
            spark = SparkSession.builder.appName("Weather Processor").getOrCreate()

            # Attempt to read a small portion of Kafka data to check the connection
            kafka_df = (
                spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", bootstrap_servers)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load()
            )

            # If the Kafka DataFrame is initialized without errors, connection is successful
            log.info(f"Kafka connection established on attempt {attempt + 1}")
            return kafka_df
        except Exception as e:
            log.warning(
                f"Kafka connection failed via Spark. Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})"
            )
            log.error(f"Error: {e}")
            await asyncio.sleep(delay)


async def read_kafka_data(kafka_df):
    """
    Processes the Kafka data stream with Spark.
    """

    # Get the Spark session from the Kafka DataFrame
    spark = kafka_df.sparkSession  

    # Define schema for the Kafka stream data
    schema = StructType(
        [
            StructField("timestamp", StringType(), True),
            StructField("total_precipitation", DoubleType(), True),
        ]
    )

    # Parse the JSON data from Kafka
    weather_data = (
        kafka_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.timestamp", "data.total_precipitation")
    )

    # Write the stream to the database
    weather_data.writeStream.outputMode("update").foreachBatch(
        store_weather_data
    ).start().awaitTermination()

    # Process the data (e.g., convert timestamp, apply watermark)
    processed_data = weather_data.select(
        to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss").alias("timestamp"),
        "total_precipitation",
    ).withWatermark("timestamp", "1 hour")

    # Create a temporary view for your streaming DataFrame
    weather_data.createOrReplaceTempView("weather_data")

    # Define and execute the SQL query to aggregate precipitation
    precipitation_summary = spark.sql("""
        SELECT 
            window(timestamp, '1 hour') AS window,
            SUM(total_precipitation) AS hourly_precipitation
        FROM 
            weather_data
        GROUP BY 
            window(timestamp, '1 hour')
        ORDER BY 
            window
    """)

    # To write the results to the console or any other sink
    query = precipitation_summary.write \
        .outputMode("update") \
        .format("console") \
        .start()

    query.awaitTermination()


def get_db_connection():
    try:
        return psycopg2.connect(
            host="postgres",
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
    except psycopg2.OperationalError as e:
        log.error(f"PostgreSQL connection error: {e}")
        raise


def store_weather_data(batch_df, batch_id):
    """
    Stores the fetched weather data into the PostgreSQL database from a Spark DataFrame.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cursor:
                # Filter out rows with None values for timestamp or total_precipitation
                filtered_df = batch_df.filter(col("timestamp").isNotNull() & col("total_precipitation").isNotNull())
                
                # Iterate over the rows of the DataFrame
                for row in filtered_df.collect():
                    cursor.execute(
                        """
                        INSERT INTO weather_data (timestamp, total_precipitation)
                        VALUES (%s, %s);
                        """,
                        (datetime.fromtimestamp(int(row.timestamp)), row.total_precipitation),
                    )
                    log.info(
                        f"Stored data in database: {datetime.fromtimestamp(int(row.timestamp))} | Precipitation: {row.total_precipitation} mm"
                    )
    except Exception as e:
        log.error(f"Error storing data in database: {e}")
    finally:
        if conn:
            conn.close()


async def main():
    kafka_df = await wait_for_kafka_connection(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    await read_kafka_data(kafka_df)


if __name__ == "__main__":
    asyncio.run(main())
