# excuser_producer.py
import json
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "kafka-stream-playground" 

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id="excuser-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
)

print("Listening for excuses...")

for msg in consumer:
    event = msg.value[0]
    category = event["category"]
    excuse = event["excuse"]
    print(f"Excuse '{category}': {excuse}")