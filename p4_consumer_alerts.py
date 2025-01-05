from kafka import KafkaConsumer
from configs import kafka_config
import json

topic_prefix = kafka_config["topic_prefix"]

consumer = KafkaConsumer(
    f"{topic_prefix}_temperature_alerts",
    f"{topic_prefix}_humidity_alerts",
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

for message in consumer:
    print(f"Received alert: {message.value}")
