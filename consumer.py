from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "stock_events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Listening to stock_events...")

for message in consumer:
    data = message.value
    print("Received:", data)
