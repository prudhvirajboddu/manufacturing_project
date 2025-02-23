import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_TOPIC = "manufacturing-data"
KAFKA_SERVER = "kafka:9092"  # Docker network name for Kafka

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def generate_sensor_data():
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "machine_id": f"M{random.randint(1, 5)}",
        "temperature": round(random.uniform(50, 100), 2),
        "vibration": round(random.uniform(0.001, 0.005), 4),
        "pressure": round(random.uniform(0.8, 1.5), 2),
        "error_code": random.choice([0, 0, 0, 1, 2])  # Simulating failures
    }

while True:
    data = generate_sensor_data()
    producer.send(KAFKA_TOPIC, data)
    print(f"Sent: {data}")
    time.sleep(1)