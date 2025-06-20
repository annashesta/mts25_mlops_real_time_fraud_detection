"""Сервис для сохранения результатов в PostgreSQL."""

import logging
import json
import psycopg2
from confluent_kafka import Consumer

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/db_service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DBService:
    def __init__(self, config):
        self.conn = psycopg2.connect(
            dbname=config['database'],
            user=config['user'],
            password=config['password'],
            host=config['host'],
            port=config['port']
        )
        self._init_db()
        self.consumer = Consumer({
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'db_service_group',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe(['scores'])

    def _init_db(self):
        """Инициализация таблицы в БД."""
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
        """Сохранение результата в БД."""
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

    def run(self):
        """Основной цикл обработки сообщений."""
        logger.info("Starting DB service...")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
                try:
                    result = json.loads(msg.value())
                    self.insert_result(result)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        except KeyboardInterrupt:
            logger.info("Stopping DB service...")
        finally:
            self.consumer.close()
            self.conn.close()

if __name__ == "__main__":
    import yaml
    with open('config/postgres_config.yaml', 'r') as f:
        POSTGRES_CONFIG = yaml.safe_load(f)
    service = DBService(POSTGRES_CONFIG)
    service.run()