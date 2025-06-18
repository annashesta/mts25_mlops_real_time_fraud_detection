import os
import json
import logging
import time
from confluent_kafka import Consumer
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ScoreLoader:
    def __init__(self):
        logger.info('Инициализация ScoreLoader...')
        self.kafka_consumer = Consumer({
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            'group.id': 'score_loader_group',
            'auto.offset.reset': 'earliest'
        })
        self.kafka_consumer.subscribe([os.getenv('KAFKA_INPUT_TOPIC')])
        self.conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            dbname=os.getenv('POSTGRES_DB')
        )
        self.cur = self.conn.cursor()
        self.create_table()
        logger.info('ScoreLoader инициализирован.')

    def create_table(self):
        """Создание таблицы для хранения результатов."""
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id TEXT PRIMARY KEY,
            score FLOAT,
            fraud_flag INT
        );
        '''
        self.cur.execute(create_table_query)
        self.conn.commit()
        logger.info('Таблица transactions создана или уже существует.')

    def process_message(self, msg):
        """Обработка одного сообщения из Kafka."""
        try:
            logger.info(f'Обработка сообщения: {msg.key()}')
            data = json.loads(msg.value())
            insert_query = '''
            INSERT INTO transactions (transaction_id, score, fraud_flag)
            VALUES (%s, %s, %s)
            ON CONFLICT (transaction_id) DO UPDATE
            SET score = EXCLUDED.score, fraud_flag = EXCLUDED.fraud_flag;
            '''
            self.cur.execute(insert_query, (data['transaction_id'], data['score'], data['fraud_flag']))
            self.conn.commit()
            logger.info('Данные записаны в PostgreSQL: %s', data)
        except Exception as e:
            logger.error(f'Ошибка обработки сообщения: {e}', exc_info=True)

    def start_consuming(self):
        """Начать чтение сообщений из Kafka."""
        try:
            while True:
                msg = self.kafka_consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f'Ошибка Kafka: {msg.error()}')
                    continue
                self.process_message(msg)
        except KeyboardInterrupt:
            logger.info('Сервис остановлен')
        finally:
            self.kafka_consumer.close()
            self.cur.close()
            self.conn.close()

if __name__ == "__main__":
    logger.info('Starting ScoreLoader service...')
    loader = ScoreLoader()
    loader.start_consuming()