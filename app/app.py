import os
import sys
import json
import logging
import time
from datetime import datetime
from confluent_kafka import Consumer, Producer
import pandas as pd
import psycopg2
from src.preprocess import run_preproc
from src.scorer import make_pred, initialize_threshold, MODEL
from src.feature_importance import save_feature_importance
from src.plot_predictions import plot_predictions_distribution

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_config(config_path):
    """Загрузка конфигурационного файла."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Ошибка загрузки конфига: {e}")
        raise

class ProcessingService:
    def __init__(self, config):
        logger.info('Инициализация ProcessingService...')
        self.config = config
        self._validate_config()
        initialize_threshold(self.config)
        self.kafka_consumer = Consumer({
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            'group.id': 'fraud_group',
            'auto.offset.reset': 'earliest'
        })
        self.kafka_consumer.subscribe([os.getenv('KAFKA_INPUT_TOPIC')])
        self.kafka_producer = Producer({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS')})
        logger.info('Сервис инициализирован.')

    def _validate_config(self):
        """Проверка корректности конфигурации."""
        required_paths = [
            'model_path',
            'threshold_path',
            'categorical_features_path'
        ]
        for path_key in required_paths:
            if not os.path.exists(self.config['paths'][path_key]):
                raise FileNotFoundError(
                    f"Путь не найден: {self.config['paths'][path_key]}"
                )

    def process_message(self, msg):
        """Обработка одного сообщения из Kafka."""
        try:
            logger.info(f'Обработка сообщения: {msg.key()}')
            input_data = json.loads(msg.value())
            input_df = pd.DataFrame([input_data])
            processed_df = run_preproc(input_df)
            submission = make_pred(processed_df, self.config)
            score = submission.iloc[0]['prediction']
            fraud_flag = 1 if score >= 0.5 else 0  # Пример порога классификации
            result = {
                'transaction_id': input_data['transaction_id'],
                'score': score,
                'fraud_flag': fraud_flag
            }
            self.kafka_producer.produce(os.getenv('KAFKA_OUTPUT_TOPIC'), json.dumps(result))
            self.kafka_producer.flush()
            logger.info('Результат отправлен в Kafka: %s', result)
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

if __name__ == "__main__":
    logger.info('Starting ML scoring service...')
    config = load_config('./config.yaml')
    service = ProcessingService(config)
    service.start_consuming()