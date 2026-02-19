"""
topics.py
Creates 3 Kafka topics if they don't exist
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

KAFKA_BROKER = "broker:9092"
TOPICS = ["raw_data", "processed_data", "anomalies"]

def create_topics():
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER, client_id="topics_creator")
    new_topics = [NewTopic(name=t, num_partitions=3, replication_factor=1) for t in TOPICS]
    try:
        admin.create_topics(new_topics=new_topics, validate_only=False)
        print(f"✓ Created topics: {TOPICS}")
    except TopicAlreadyExistsError:
        print(f"✓ Topics already exist")
    finally:
        admin.close()

if __name__ == "__main__":
    create_topics()
