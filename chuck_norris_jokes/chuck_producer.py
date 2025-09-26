from kafka import KafkaProducer
import json
import requests
import time

KAFKA_TOPIC = "chuck-norris-jokes"
BASE_URL = "https://api.chucknorris.io"

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def main():
    while True:
        try:
            r = requests.get(f"{BASE_URL}/jokes/random")
            r.raise_for_status()
            joke = r.json()
            producer.send(KAFKA_TOPIC, joke)
            producer.flush()
            print(f"Sent joke to topic {KAFKA_TOPIC}")
            time.sleep(10)
        except requests.RequestException as e:
            print(f"Error fetching joke: {e}")

if __name__ == "__main__":
    main()