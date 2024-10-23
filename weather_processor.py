import os
import sys
import logging
import asyncio
import psycopg2
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_json, col, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
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

    Parameters:
    ----------
    bootstrap_servers : str
        The Kafka bootstrap servers to connect to, formatted as "host:port".
    topic : str
        The Kafka topic to subscribe to for reading data.
    retries : int, optional
        The number of times to retry connecting to Kafka if the initial connection fails (default is 10).
    delay : int, optional
        The delay in seconds between retry attempts (default is 2 seconds).

    Returns:
    -------
    pyspark.sql.dataframe.DataFrame
        A DataFrame representing the Kafka stream if the connection is successful.

    Raises:
    -------
    Exception
        If the connection cannot be established after the specified number of retries.
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

    raise Exception("Failed to connect to Kafka after multiple attempts.")

async def read_kafka_data(kafka_df):
    """
    Processes the Kafka data stream with Spark.

    This function reads the incoming Kafka data, parses it according to the specified schema,
    aggregates the total precipitation by timestamp and location, and sets up a streaming query
    to output the results to the console and store them in a PostgreSQL database.

    Parameters:
    ----------
    kafka_df : pyspark.sql.dataframe.DataFrame
        A Spark DataFrame representing the incoming Kafka stream. This DataFrame should contain
        the raw data from Kafka, which includes JSON-encoded values.

    Returns:
    -------
    None
        This function does not return a value. It sets up a streaming query that runs indefinitely
        until stopped.

    Raises:
    -------
    Exception
        Any exceptions that occur during the processing of the Kafka stream or while writing to
        the database will be raised and logged.
    """

    # Specify the updated schema to match producer data
    schema = StructType(
        [
            StructField("timestamp", StringType(), True),
            StructField(
                "total_precipitation", DoubleType(), True
            ),  # Total precipitation
            StructField(
                "location",
                StructType(
                    [  # Location as nested struct
                        StructField("lat", DoubleType(), True),  # Latitude
                        StructField("lon", DoubleType(), True),  # Longitude
                    ]
                ),
            ),
        ]
    )

    # Parse the JSON data from Kafka
    weather_data = (
        kafka_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select(
            "data.timestamp",
            "data.total_precipitation",
            "data.location.lat",
            "data.location.lon",
        )
    )

    # Group by Timestamp and Location and SUM the precipitation for each hour
    precipitation_summary = weather_data.groupBy("timestamp", "lat", "lon").agg(
        sum("total_precipitation").alias("hourly_precipitation")
    )

    # Display the result to console or store in the database
    query = (
        precipitation_summary.writeStream.outputMode("update")
        .format("console")
        .foreachBatch(store_weather_data)
        .start()
    )

    query.awaitTermination()


def get_db_connection(retries=5, delay=5):
    """
    Establishes a connection to the PostgreSQL database with retry mechanism.

    This function attempts to connect to the PostgreSQL database specified by the 
    global connection parameters. If the connection fails, it will retry the specified 
    number of times with a delay between attempts.

    Parameters:
    ----------
    retries : int, optional
        The number of times to retry connecting to the database. Default is 5.
    
    delay : int, optional
        The number of seconds to wait between retries. Default is 5.

    Returns:
    -------
    connection : psycopg2.extensions.connection
        A connection object to the PostgreSQL database if the connection is successful.

    Raises:
    -------
    Exception
        Raises an exception if the connection fails after the specified number of retries.
    """
    for attempt in range(retries):
        try:
            return psycopg2.connect(
                host="postgres",
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
        except psycopg2.OperationalError as e:
            log.error(f"PostgreSQL connection error (attempt {attempt + 1}/{retries}): {e}")
            time.sleep(delay)
    raise Exception("Failed to connect to PostgreSQL after multiple attempts.")


def store_weather_data(batch_df, batch_id):
    """
    Stores the fetched weather data into the PostgreSQL database from a Spark DataFrame.

    This function takes a batch of weather data, filters out any rows with 
    missing values for key fields, and inserts the valid data into the 
    PostgreSQL database. Each insertion is logged for tracking purposes.

    Parameters:
    ----------
    batch_df : pyspark.sql.DataFrame
        A Spark DataFrame containing the weather data to be stored. It should include 
        columns for timestamp, latitude, longitude, and hourly precipitation.
    
    batch_id : int
        The identifier for the batch of data being processed. This is primarily 
        for logging purposes and can be used to track which batch is being stored.

    Returns:
    -------
    None

    Raises:
    -------
    Exception
        Raises an exception if there is an error during the database connection 
        or data insertion process.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cursor:
                # Filter out rows with None values for timestamp or precipitation
                filtered_df = batch_df.filter(
                    col("timestamp").isNotNull()
                    & col("hourly_precipitation").isNotNull()
                    & col("lat").isNotNull()
                    & col("lon").isNotNull()
                )

                # Iterate over the rows of the DataFrame
                for row in filtered_df.collect():
                    cursor.execute(
                        """
                        INSERT INTO weather_data (timestamp, lat, lon, hourly_precipitation)
                        VALUES (%s, %s, %s, %s);
                        """,
                        (
                            datetime.fromtimestamp(int(row.timestamp)),
                            row.lat,
                            row.lon,
                            row.hourly_precipitation,
                        ),
                    )
                    log.info(
                        f"Stored data in database: {datetime.fromtimestamp(int(row.timestamp))} | "
                        f"Location: ({row.lat}, {row.lon}) | Precipitation: {row.hourly_precipitation} mm"
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
