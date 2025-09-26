import json
import time
import requests
from kafka import KafkaProducer

KAFKA_TOPIC = "kafka-stream-playground"
BASE_URL = "https://icanhazdadjoke.com/"
KAFKA_BOOTSTRAP= 'localhost:9092'

def fetch_joke():
    url = BASE_URL
    headers = {'Accept': 'application/json'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    while True:
        try:
            joke = fetch_joke()
            producer.send(KAFKA_TOPIC, joke)
            producer.flush()
            print(f"Sent joke to topic {KAFKA_TOPIC}")
            time.sleep(10)
        except requests.RequestException as e:
            print(f"Error fetching joke: {e}")

if __name__ == "__main__":
    main()