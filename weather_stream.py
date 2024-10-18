import requests
import time
import json
from kafka import KafkaProducer

# Constants
API_KEY = '01b46e650cb36bc0395af11be491cd78'
LAT = '52.084516'
LON = '5.115539'
URL = f"https://api.openweathermap.org/data/3.0/onecall?lat={LAT}&lon={LON}&exclude=hourly,daily,current&units=metric&appid={API_KEY}"
KAFKA_TOPIC = 'weather_data'
CALL_INTERVAL = 60  # seconds

# Wait for Kafka to start
time.sleep(5)  # Wait for 5 seconds before starting the producer

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='kafka:9092',  # Change localhost to kafka
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_weather_data():
    """Fetches weather data from OpenWeatherMap API and sends it to Kafka."""
    try:
        response = requests.get(URL)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()
        print(data)
        
        if 'minutely' in data:
            precip_data = data['minutely']
            total_precip = sum([minute.get('precipitation', 0) for minute in precip_data])
            timestamp = precip_data[0]['dt']

            # Send data to Kafka
            producer.send(KAFKA_TOPIC, {'timestamp': timestamp, 'total_precipitation': total_precip})
            print(f"Sent data: {timestamp} | Precipitation: {total_precip} mm")
        else:
            print("No 'minutely' data available")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")

if __name__ == "__main__":
    while True:
        fetch_weather_data()
        time.sleep(CALL_INTERVAL)  # Wait for 60 seconds before the next API call