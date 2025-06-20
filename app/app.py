"""Основной скрипт приложения."""

import logging
import json
import time
from kafka import KafkaConsumer, KafkaProducer
from src.preprocess import run_preproc
from src.scorer import load_model, load_threshold, load_categorical_features
from config import load_config  # Добавьте функцию для загрузки конфига
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def wait_for_kafka():
    # Ваш код
    pass

def main():
    # Загрузка конфигов
    kafka_config = load_config('config/kafka_config.yaml')
    postgres_config = load_config('config/postgres_config.yaml')
    
    # Инициализация модели
    model_path = '/app/model/catboost_model.cbm'
    categorical_features_path = '/app/model/categorical_features.json'
    threshold_path = '/app/model/threshold.json'
    
    categorical_features = load_categorical_features(categorical_features_path)
    model = load_model(model_path, categorical_features)
    threshold = load_threshold(threshold_path)
    
    # Создание потребителя и производителя Kafka
    consumer = KafkaConsumer(
        kafka_config['input_topic'],
        bootstrap_servers=kafka_config['kafka_bootstrap_servers'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='fraud-detector-group'
    )
    producer = KafkaProducer(
        bootstrap_servers=kafka_config['kafka_bootstrap_servers'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Основной цикл
    for message in consumer:
        try:
            transaction = json.loads(message.value.decode('utf-8'))
            # Преобразование в DataFrame
            df = pd.DataFrame([transaction])
            processed_df = run_preproc(df)
            score = model.predict_proba(processed_df)[0][1]
            fraud_flag = score >= threshold
            
            result = {
                "transaction_id": transaction["transaction_id"],
                "score": float(score),
                "fraud_flag": bool(fraud_flag)
            }
            producer.send(kafka_config['output_topic'], value=result)
        except Exception as e:
            logger.error(f"Ошибка: {e}", exc_info=True)