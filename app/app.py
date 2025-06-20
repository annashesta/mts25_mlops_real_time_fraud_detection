"""Основной скрипт приложения."""

import logging
import json
import time
from kafka import KafkaConsumer, KafkaProducer
from src.preprocess import preprocess_data
from src.scorer import Scorer
from app.db_service import DBService
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, INPUT_TOPIC, OUTPUT_TOPIC
from config.postgres_config import POSTGRES_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def wait_for_kafka():
    max_retries = 10
    retry_count = 0
    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='fraud-detector-group'
            )
            consumer.close()
            return True
        except Exception as e:
            logger.warning(f"Waiting for Kafka... Attempt {retry_count + 1}/{max_retries}")
            time.sleep(5)
            retry_count += 1
    raise Exception("Failed to connect to Kafka after multiple attempts")

def main():
    wait_for_kafka()
    
    scorer = Scorer()
    db_service = DBService(POSTGRES_CONFIG)
    db_service.create_table()

    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='fraud-detector-group'
    )
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    logger.info("Fraud detection service started")
    
    try:
        for message in consumer:
            try:
                transaction = json.loads(message.value.decode('utf-8'))
                logger.info(f"Processing transaction: {transaction['transaction_id']}")
                
                processed_data = preprocess_data(transaction)
                score = scorer.predict(processed_data)
                fraud_flag = score > scorer.threshold

                result = {
                    "transaction_id": transaction["transaction_id"],
                    "score": float(score),
                    "fraud_flag": bool(fraud_flag)
                }

                producer.send(OUTPUT_TOPIC, value=result)
                db_service.insert_result(result)
                
                logger.info(f"Processed transaction: {transaction['transaction_id']}, score: {score:.4f}, fraud: {fraud_flag}")
                
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        consumer.close()
        producer.close()
        db_service.conn.close()

if __name__ == "__main__":
    main()