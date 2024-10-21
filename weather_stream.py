import os
import sys
import requests
import logging
import json
from kafka import KafkaProducer
from datetime import datetime
import asyncio
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

log = logging.getLogger(__name__)

# Constants
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")

API_KEY = os.environ.get("OPENWEATHER_API_KEY")
LAT = os.environ.get("LAT")
LON = os.environ.get("LON")


URL = f"https://api.openweathermap.org/data/3.0/onecall?lat={LAT}&lon={LON}&exclude=hourly,daily,current&units=metric&appid={API_KEY}"
CALL_INTERVAL = 60  # seconds


async def wait_for_kafka_connection(bootstrap_servers, retries=10, delay=2):
    """
    Asynchronously tries to establish a connection to Kafka with a retry mechanism.
    """
    for attempt in range(retries):
        try:
            # Initialize Kafka producer
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                compression_type=os.environ.get("KAFKA_PRODUCER_COMPRESSION_TYPE"),
                retries=int(
                    os.environ.get("KAFKA_PRODUCER_RETRIES")
                ),  # Retry up to X times on failure
                batch_size=128,
                linger_ms=50,
                acks=os.environ.get("KAFKA_PRODUCER_ACKS"),
                request_timeout_ms=30000,  # Set timeout to 30 seconds
                metadata_max_age_ms=60000,  # Allow 60 seconds to refresh metadata
            )
            log.info(f"Kafka producer connection established on attempt {attempt + 1}")
            return producer
        except NoBrokersAvailable:
            log.warning(
                f"Kafka producer connection failed. Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})"
            )
            await asyncio.sleep(delay)

    raise Exception("Failed to connect to Kafka after multiple attempts.")


def fetch_weather_data(producer):
    """
    Fetches weather data from the OpenWeatherMap API and sends it to a Kafka topic.
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
            record = producer.send(
                KAFKA_TOPIC,
                {"timestamp": timestamp, "total_precipitation": total_precip},
            )

            # Wait for the Kafka acknowledgment
            try:
                record_metadata = record.get(timeout=30)  # Wait for acknoledge
                log.info(
                    f"Message sent successfully to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}. Sent data: {datetime.fromtimestamp(timestamp)} | Precipitation: {total_precip} mm"
                )
            except Exception as e:
                log.error(f"Failed to send message: {e}")
        else:
            log.info("No 'minutely' data available")
    except requests.exceptions.RequestException as e:
        log.error(f"Error fetching data: {e}")


async def main():
    producer = await wait_for_kafka_connection(KAFKA_BOOTSTRAP_SERVERS)

    while True:
        fetch_weather_data(producer)
        await asyncio.sleep(
            CALL_INTERVAL
        )  # Wait for 60 seconds before the next API call

    # Close the connection when done
    producer.close()


if __name__ == "__main__":
    asyncio.run(main())
