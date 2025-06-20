"""Основной скрипт приложения."""

import logging
import json
from src.consumer import consume_messages
from src.preprocess import preprocess_data
from src.scorer import Scorer
from app.db_service import DBService
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, INPUT_TOPIC, OUTPUT_TOPIC
from config.postgres_config import POSTGRES_CONFIG

def main():
    scorer = Scorer()
    db_service = DBService(POSTGRES_CONFIG)
    db_service.create_table()

    for message in consume_messages(KAFKA_BOOTSTRAP_SERVERS, INPUT_TOPIC):
        transaction = json.loads(message.value.decode('utf-8'))
        processed_data = preprocess_data(transaction)
        score = scorer.predict(processed_data)
        fraud_flag = score > scorer.threshold

        result = {
            "transaction_id": transaction["transaction_id"],
            "score": score,
            "fraud_flag": fraud_flag
        }

        produce_message(KAFKA_BOOTSTRAP_SERVERS, OUTPUT_TOPIC, result)
        db_service.insert_result(result)

def produce_message(bootstrap_servers, topic, message):
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer.send(topic, json.dumps(message).encode('utf-8'))
    producer.flush()

if __name__ == "__main__":
    main()