import json
from kafka import KafkaConsumer

KAFKA_TOPIC = "chuck-norris-jokes"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='chuck-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for msg in consumer:
    joke = msg.value
    print(f"Received joke: {joke['value']}")