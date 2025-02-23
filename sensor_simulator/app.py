import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)

KAFKA_SERVER = "kafka:9092"
KAFKA_TOPIC = "manufacturing-data"

# Retry Kafka connection
def create_kafka_producer():
    for _ in range(10):  # Retry 10 times
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logging.info("✅ Connected to Kafka successfully!")
            return producer
        except Exception as e:
            logging.error(f"❌ Kafka connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)
    raise Exception("Kafka connection failed after multiple attempts")

producer = create_kafka_producer()

def generate_sensor_data():
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "machine_id": f"M{random.randint(1, 5)}",
        "temperature": round(random.uniform(50, 100), 2),
        "vibration": round(random.uniform(0.001, 0.005), 4),
        "pressure": round(random.uniform(0.8, 1.5), 2),
        "error_code": random.choice([0, 0, 0, 1, 2])
    }

while True:
    data = generate_sensor_data()
    producer.send(KAFKA_TOPIC, data)
    logging.info(f"📡 Sent: {data}")
    time.sleep(1)
