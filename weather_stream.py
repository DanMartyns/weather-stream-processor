# This file follows the Ruff format

import os
import sys
import requests
import logging
import time
import json
import psycopg2
from kafka import KafkaProducer
from datetime import datetime
import asyncio
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format="%(asctime)s - %(levelname)s - %(message)s",  # Format with timestamps
    handlers=[
        logging.StreamHandler(sys.stdout)  # Log to stdout to be captured by Docker
    ],
)

log = logging.getLogger(__name__)

# Constants
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")

API_KEY = os.environ.get("OPENWEATHER_API_KEY")
LAT = os.environ.get("LAT")
LON = os.environ.get("LON")

POSTGRES_DB = os.environ.get("POSTGRES_DB")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")

URL = f"https://api.openweathermap.org/data/3.0/onecall?lat={LAT}&lon={LON}&exclude=hourly,daily,current&units=metric&appid={API_KEY}"
CALL_INTERVAL = 60  # seconds


async def wait_for_kafka_connection(bootstrap_servers, retries=5, delay=2):
    """
    Asynchronously tries to establish a connection to Kafka with a retry mechanism.

    Args:
        bootstrap_servers (str): Kafka bootstrap server(s) to connect to.
        retries (int): Number of times to retry the connection. Default is 5.
        delay (int): Time in seconds to wait between retry attempts. Default is 2 seconds.

    Returns:
        KafkaProducer: A Kafka producer instance if connection is successful.

    Raises:
        Exception: If the connection could not be established after the specified retries.
    """
    for attempt in range(retries):
        try:
            # Initialize Kafka producer
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                key_serializer=lambda k: k.encode('utf-8') if isinstance(k, str) else k,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks=os.environ.get("KAFKA_PRODUCER_ACKS"),
                compression_type=os.environ.get("KAFKA_PRODUCER_COMPRESSION_TYPE"),
                retries=int(os.environ.get("KAFKA_PRODUCER_RETRIES")),
                max_in_flight_requests_per_connection=int(os.environ.get("KAFKA_PRODUCER_MAX_IN_FLIGHT_REQUESTS")),
                batch_size=16384,  # 16 KB batch size
                linger_ms=10  # Wait for up to 10ms to fill the batch
            )
            log.info(f"Kafka connection established on attempt {attempt + 1}")
            return producer
        except NoBrokersAvailable:
            log.warning(
                f"Kafka connection failed. Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})"
            )
            await asyncio.sleep(delay)

    raise Exception("Failed to connect to Kafka after multiple attempts.")


producer = asyncio.run(wait_for_kafka_connection("kafka:9092"))


def fetch_weather_data():
    """
    Fetches weather data from the OpenWeatherMap API, calculates total precipitation for the past hour,
    and sends the result to a Kafka topic.

    The function makes an HTTP GET request to the OpenWeatherMap API, processes the 'minutely' precipitation
    data if available, computes the total precipitation (in millimeters) over the last 60 minutes, and sends
    the calculated value along with its timestamp to the configured Kafka topic.

    Logs:
    - Success log: Records the timestamp and total precipitation sent to Kafka.
    - No data log: Indicates when no 'minutely' data is available in the API response.
    - Error log: Captures any request-related errors encountered during the API call.

    Raises:
    - RequestsException: If the API request fails or returns a non-200 response.
    """
    try:
        response = requests.get(URL)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()

        if "minutely" in data:
            precip_data = data["minutely"]
            total_precip = round(
                sum([minute.get("precipitation", 0) for minute in precip_data]), 5
            )
            timestamp = precip_data[0]["dt"]

            # Send data to Kafka
            producer.send(
                KAFKA_TOPIC,
                {"timestamp": timestamp, "total_precipitation": total_precip},
            )
            log.info(
                f"Sent data: {datetime.fromtimestamp(timestamp)} | Precipitation: {total_precip} mm"
            )
            # Store data in PostgreSQL
            store_weather_data(timestamp, total_precip)
        else:
            log.info("No 'minutely' data available")
    except requests.exceptions.RequestException as e:
        log.error(f"Error fetching data: {e}")


def get_db_connection():
    return psycopg2.connect(
        host="postgres",
        database="weather_db",
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    return conn


def store_weather_data(timestamp, total_precipitation):
    """
    Stores the fetched weather data into the PostgreSQL database.

    Args:
        timestamp (int): The timestamp of the precipitation data.
        total_precipitation (float): The total precipitation value to store.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO weather_data (timestamp, total_precipitation)
                    VALUES (%s, %s);
                    """,
                    (datetime.fromtimestamp(timestamp), total_precipitation),
                )
        log.info(f"Stored data in database: {datetime.fromtimestamp(timestamp)} | Precipitation: {total_precipitation} mm")
    except Exception as e:
        log.error(f"Error storing data in database: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    while True:
        fetch_weather_data()
        time.sleep(CALL_INTERVAL)  # Wait for 60 seconds before the next API call
