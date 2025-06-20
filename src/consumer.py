"""Консьюмер Kafka для чтения сообщений в CSV формате."""

import json
import csv
from io import StringIO
from kafka import KafkaConsumer

def consume_messages(bootstrap_servers, topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='fraud-detection-group'
    )
    
    for message in consumer:
        try:
            # Попробуем сначала как JSON
            try:
                data = json.loads(message.value.decode('utf-8'))
                yield data
            except json.JSONDecodeError:
                # Если не JSON, обрабатываем как CSV
                csv_data = StringIO(message.value.decode('utf-8'))
                reader = csv.DictReader(csv_data)
                for row in reader:
                    yield row
        except Exception as e:
            print(f"Error processing message: {e}")
            continue