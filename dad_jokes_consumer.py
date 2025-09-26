import json
from kafka import KafkaConsumer

KAFKA_TOPIC = "kafka-stream-playground"
BASE_URL = "https://icanhazdadjoke.com/"
KAFKA_BOOTSTRAP= 'localhost:9092'

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='dad-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for msg in consumer:
        joke = msg.value
        print(f"Received joke: {joke['joke']}")

if __name__ == "__main__":
    main()