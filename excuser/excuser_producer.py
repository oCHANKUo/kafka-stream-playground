# excuser_producer.py
import time
import json
import requests
from kafka import KafkaProducer

KAFKA_TOPIC = "kafka-stream-playground"
BASE_URL = "https://excuser-three.vercel.app/v1/excuse/"
KAFKA_BOOTSTRAP= 'localhost:9092'

CATEGORIES = ["office", "children", "college", "party", "funny", "family", "unbelievable", "gaming", "developers"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_excuse(category):
    url = f"{BASE_URL}{category}"
    try:
        r = requests.get(url)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"Error fetching excuse: {e}")
        return {"error": str(e)}

def main():
    while True:
        for category in CATEGORIES:
            excuse = fetch_excuse(category)
            producer.send(KAFKA_TOPIC, excuse)
            producer.flush()
            print(f"Sent excuse from category '{category}'")
            time.sleep(5) 

if __name__ == "__main__":
    main()

