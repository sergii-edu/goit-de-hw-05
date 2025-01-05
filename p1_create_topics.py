from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)

topic_prefix = kafka_config["topic_prefix"]

topics = [
    NewTopic(
        name=f"{topic_prefix}_building_sensors", num_partitions=3, replication_factor=1
    ),
    NewTopic(
        name=f"{topic_prefix}_temperature_alerts",
        num_partitions=3,
        replication_factor=1,
    ),
    NewTopic(
        name=f"{topic_prefix}_humidity_alerts", num_partitions=3, replication_factor=1
    ),
]

try:
    admin_client.create_topics(new_topics=topics, validate_only=False)
    print("Topics created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

[print(topic) for topic in admin_client.list_topics() if topic_prefix in topic]

admin_client.close()
