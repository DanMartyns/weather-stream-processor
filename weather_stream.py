import os
import sys
import requests
import logging
import json
from kafka import KafkaProducer
from datetime import datetime
import asyncio

logging.basicConfig(
    level=logging.WARN,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

log = logging.getLogger(__name__)

# Constants
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")

API_KEY = os.environ.get("OPENWEATHER_API_KEY")
LOCATIONS_JSON = os.environ.get("LOCATIONS_JSON")
LOCATIONS = json.loads(LOCATIONS_JSON) if LOCATIONS_JSON else []

CALL_INTERVAL = 60  # seconds


def build_url(lat, lon):
    return f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude=hourly,daily,current&units=metric&appid={API_KEY}"


async def wait_for_kafka_connection(bootstrap_servers, retries=10, delay=2):
    """
    Asynchronously tries to establish a connection to Kafka with a retry mechanism.

    This function attempts to create a Kafka producer connection to the specified 
    bootstrap servers. It includes a retry mechanism to handle transient connection 
    issues, logging each attempt and its outcome.

    Parameters:
    ----------
    bootstrap_servers : str
        A comma-separated string of Kafka bootstrap server addresses. 
        Example: "localhost:9092,localhost:9094".

    retries : int, optional
        The maximum number of attempts to connect to Kafka. Default is 10.

    delay : int, optional
        The number of seconds to wait between connection attempts. Default is 2 seconds.

    Returns:
    -------
    KafkaProducer
        An instance of KafkaProducer if the connection is successful.

    Raises:
    -------
    Exception
        Raises an exception if a connection to Kafka cannot be established 
        after the specified number of retries.
    """
    for attempt in range(retries):
        try:
            # Initialize Kafka producer
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers, # Kafka broker addresses
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k, # Serialize function. Converts string keys to UTF-8 encode bytes.
                value_serializer=lambda v: json.dumps(v).encode("utf-8"), # Serialize function. Converts the value to a JSON string and then encodes it as UTF-8 bytes.
                retries=5,  # Sets the number of retry attempts the producer will make if sending a message fails.
                batch_size=16384, # Defines the maximum size (in bytes) of a batch of messages that the producer will send to Kafka in one request. (16 kB)
                linger_ms=0, # Specifies the time (in milliseconds) to wait before sending a batch of messages. Setting to 0, messages will be sent immediately without waiting.
                acks='all', # Defines the number of acknowledgments the producer requires from Kafka before considering a request complete.
                request_timeout_ms=60000,  # Sets the maximum time (in milliseconds) the producer will wait for a response from Kafka before timing out. (60 sec)
                metadata_max_age_ms=60000,  # Defines how long (in milliseconds) the producer should cache metadata about the brokers. (60 sec)
            )
            log.info(f"Kafka producer connection established on attempt {attempt + 1}")
            return producer
        except Exception as e:
            log.warning(
                f"Kafka producer connection failed. Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})"
            )
            log.error(f"Error: {e}")
            await asyncio.sleep(delay)

    raise Exception("Failed to connect to Kafka after multiple attempts.")


async def fetch_weather_data(producer):
    """
    Fetches weather data from the OpenWeatherMap API for specified locations and sends it to a Kafka topic.

    This function iterates over predefined locations, constructs the API request URL for each location,
    and retrieves weather data asynchronously. It extracts precipitation data from the response, calculates 
    the total precipitation, and sends this information to the specified Kafka topic. The function also 
    logs the status of the sent message and handles potential errors during the fetching and sending process.

    Parameters:
        producer (KafkaProducer): An instance of the KafkaProducer used to send messages to the Kafka topic.

    Raises:
        requests.exceptions.RequestException: If there is an error during the API request.

    Notes:
        - It waits for an acknowledgment from Kafka after sending each message and logs success or failure 
          accordingly.
    """

    for location in LOCATIONS:
        lat = location["lat"]
        lon = location["lon"]
        url = build_url(lat, lon)

        try:
            response = await asyncio.to_thread(requests.get, url)
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
                    {
                        "timestamp": timestamp,
                        "total_precipitation": total_precip,
                        "location": {"lat": lat, "lon": lon},
                    },
                )

                # Wait for the Kafka acknowledgment
                try:
                    record_metadata = record.get(timeout=30)  # Wait for acknoledge
                    log.info(
                        f"Message sent successfully to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}. Sent data: {datetime.fromtimestamp(timestamp)} | Precipitation: {total_precip} mm | Latitude: {lat} | Longitude: {lon}"
                    )
                except Exception as e:
                    log.error(f"Failed to send message: {e}")
            else:
                log.info("No 'minutely' data available for location {lat}, {lon}")
        except requests.exceptions.RequestException as e:
            log.error(f"Error fetching data for location {lat}, {lon}: {e}")


async def main():
    producer = await wait_for_kafka_connection(KAFKA_BOOTSTRAP_SERVERS)

    while True:
        await fetch_weather_data(producer)
        await asyncio.sleep(CALL_INTERVAL)

    producer.close()


if __name__ == "__main__":
    asyncio.run(main())
