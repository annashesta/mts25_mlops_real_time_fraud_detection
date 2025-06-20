"""Модуль для предобработки данных перед подачей в модель."""

import logging
from typing import List
import pandas as pd
import numpy as np
from geopy.distance import great_circle
from sklearn.impute import SimpleImputer

# Настройка логгера
logger = logging.getLogger(__name__)

# Глобальные переменные (будут инициализированы в load_train_data)
categorical_cols: List[str] = []
continuous_cols: List[str] = ["amount"]

def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Добавляет временные признаки из столбца 'transaction_time'.
    Временные признаки: час, год, месяц, день месяца, день недели.
    Удаляет исходный столбец 'transaction_time'.
    Args:
        df (pd.DataFrame): DataFrame с колонкой 'transaction_time'.
    Returns:
        pd.DataFrame: DataFrame с новыми временными признаками.
    """
    logger.debug("Добавление временных признаков...")
    df["transaction_time"] = pd.to_datetime(df["transaction_time"])
    dt = df["transaction_time"].dt
    df["hour"] = dt.hour
    df["year"] = dt.year
    df["month"] = dt.month
    df["day_of_month"] = dt.day
    df["day_of_week"] = dt.dayofweek
    return df.drop(columns="transaction_time")

def add_distance_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Рассчитывает расстояние между клиентом и продавцом в километрах.
    Удаляет исходные колонки с координатами.
    Args:
        df (pd.DataFrame): DataFrame с координатами клиента и продавца.
    Returns:
        pd.DataFrame: DataFrame с новым признаком 'distance'.
    """
    logger.debug("Расчет расстояний...")
    def calc_distance(row):
        try:
            return great_circle(
                (row["client_lat"], row["client_lon"]),
                (row["seller_lat"], row["seller_lon"])
            ).km
        except Exception as e:
            logger.warning(f"Ошибка расчета расстояния: {e}")
            return np.nan
    df["distance"] = df.apply(calc_distance, axis=1)
    return df.drop(columns=["client_lat", "client_lon", "seller_lat", "seller_lon"], errors="ignore")

def run_preproc(input_data: dict) -> pd.DataFrame:
    """
    Выполняет полный препроцессинг данных.
    Args:
        input_data (dict): Входные данные для предобработки.
    Returns:
        pd.DataFrame: Предобработанный DataFrame.
    """
    logger.info("Начало препроцессинга данных...")
    # Преобразование входных данных в DataFrame
    input_df = pd.DataFrame([input_data])
    # Проверка наличия необходимых колонок
    required_cols = (
        ["transaction_time", "client_lat", "client_lon", "seller_lat", "seller_lon"] +
        categorical_cols +
        continuous_cols
    )
    missing_cols = [col for col in required_cols if col not in input_df.columns]
    if missing_cols:
        raise ValueError(f"Отсутствуют необходимые колонки: {missing_cols}")
    # 1. Добавление временных признаков
    input_df = add_time_features(input_df)
    logger.info("Добавлены временные признаки.")
    # 2. Расчет расстояний
    input_df = add_distance_features(input_df)
    logger.info("Расчет расстояний завершен.")
    # 3. Обработка пропущенных значений
    continuous_cols_with_distance = continuous_cols + ["distance"]
    imputer = SimpleImputer(strategy="mean")
    input_df[continuous_cols_with_distance] = imputer.fit_transform(input_df[continuous_cols_with_distance])
    logger.info("Обработка пропущенных значений завершена.")
    # 4. Логарифмическое преобразование числовых признаков
    for col in continuous_cols_with_distance:
        input_df[f"{col}_log"] = np.log(input_df[col] + 1)
    logger.info("Логарифмическое преобразование выполнено.")
    logger.info("Препроцессинг завершен.")
    return input_df