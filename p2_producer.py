from kafka import KafkaProducer
from configs import kafka_config
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

topic_prefix = kafka_config["topic_prefix"]

sensor_id = random.randint(1000, 9999)
topic_name = f"{topic_prefix}_building_sensors"

while True:
    data = {
        "sensor_id": sensor_id,
        "timestamp": time.time(),
        "temperature": random.uniform(25, 45),
        "humidity": random.uniform(15, 85),
    }
    producer.send(topic_name, value=data)
    print(f"Sent: {data}")
    time.sleep(2)
