"""Консьюмер  Kafka для чтения сообщений."""

from kafka import KafkaConsumer

def consume_messages(bootstrap_servers, topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group'
    )
    return consumer