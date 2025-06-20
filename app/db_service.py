"""Сервис для сохранения результатов в PostgreSQL."""

import logging
import json
import time
import psycopg2
from confluent_kafka import Consumer, KafkaException
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, OUTPUT_TOPIC
from config.postgres_config import POSTGRES_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/db_service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def wait_for_services():
    max_retries = 10
    retry_count = 0
    
    # Wait for PostgreSQL
    while retry_count < max_retries:
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            conn.close()
            break
        except Exception as e:
            logger.warning(f"Waiting for PostgreSQL... Attempt {retry_count + 1}/{max_retries}")
            time.sleep(5)
            retry_count += 1
    else:
        raise Exception("Failed to connect to PostgreSQL after multiple attempts")
    
    # Wait for Kafka
    retry_count = 0
    while retry_count < max_retries:
        try:
            consumer = Consumer({
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'group.id': 'db-service-group',
                'auto.offset.reset': 'earliest'
            })
            consumer.close()
            break
        except Exception as e:
            logger.warning(f"Waiting for Kafka... Attempt {retry_count + 1}/{max_retries}")
            time.sleep(5)
            retry_count += 1
    else:
        raise Exception("Failed to connect to Kafka after multiple attempts")

class DBService:
    def __init__(self, config):
        self.config = config
        self.conn = self._connect_to_db()
        self._init_db()
        
    def _connect_to_db(self):
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                conn = psycopg2.connect(**self.config)
                logger.info("Connected to PostgreSQL")
                return conn
            except Exception as e:
                logger.warning(f"Failed to connect to PostgreSQL (attempt {retry_count + 1}/{max_retries}): {e}")
                time.sleep(5)
                retry_count += 1
        
        raise Exception("Failed to connect to PostgreSQL after multiple attempts")

    def _init_db(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id VARCHAR(50) PRIMARY KEY,
                    score FLOAT NOT NULL,
                    fraud_flag BOOLEAN NOT NULL,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.conn.commit()
        logger.info("Database initialized")

    def insert_result(self, result):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO transactions (transaction_id, score, fraud_flag)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (transaction_id) DO UPDATE
                    SET score = EXCLUDED.score, fraud_flag = EXCLUDED.fraud_flag
                """, (result['transaction_id'], result['score'], result['fraud_flag']))
                self.conn.commit()
            logger.debug(f"Saved transaction {result['transaction_id']}")
        except Exception as e:
            logger.error(f"Error saving transaction: {e}")
            self.conn.rollback()
            raise

    def run(self):
        wait_for_services()
        
        consumer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'db-service-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([OUTPUT_TOPIC])

        logger.info("Starting DB service...")
        
        try:
            while True:
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                try:
                    result = json.loads(msg.value().decode('utf-8'))
                    self.insert_result(result)
                    consumer.commit(msg)
                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            consumer.close()
            self.conn.close()
            logger.info("DB service stopped")

if __name__ == "__main__":
    service = DBService(POSTGRES_CONFIG)
    service.run()