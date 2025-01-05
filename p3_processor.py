from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json

topic_prefix = kafka_config["topic_prefix"]

consumer = KafkaConsumer(
    f"{topic_prefix}_building_sensors",
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

for message in consumer:
    data = message.value
    alerts = []

    if data["temperature"] > 40:
        alerts.append(
            {
                "sensor_id": data["sensor_id"],
                "timestamp": data["timestamp"],
                "value": data["temperature"],
                "message": "High temperature detected!",
            }
        )

    if data["humidity"] > 80 or data["humidity"] < 20:
        alerts.append(
            {
                "sensor_id": data["sensor_id"],
                "timestamp": data["timestamp"],
                "value": data["humidity"],
                "message": "Humidity out of bounds!",
            }
        )

    for alert in alerts:
        topic_name = (
            f"{topic_prefix}_temperature_alerts"
            if "temperature" in alert["message"]
            else f"{topic_prefix}_humidity_alerts"
        )
        producer.send(topic_name, value=alert)
        print(f"Alert sent: {topic_name}: {alert}")
