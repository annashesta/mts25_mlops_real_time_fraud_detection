# Базовый образ
FROM python:3.10-slim

# Установка рабочей директории
WORKDIR /app

# Создание структуры директорий
RUN mkdir -p /app/model /app/train_data /app/input /app/output /app/logs

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    rm -rf /root/.cache

# Копирование артефактов модели (локальные файлы)
COPY model/catboost_model.cbm /app/model/
COPY model/threshold.json /app/model/
COPY model/categorical_features.json /app/model/

# Копирование остальных файлов
COPY config/ /app/config/
COPY src/ /app/src/
COPY app/ /app/app/

# Настройка прав
RUN mkdir -p /app/logs && \
    useradd -m appuser && \
    chown -R appuser:appuser /app && \
    chmod -R 755 /app/logs

USER appuser

# Команда запуска
CMD ["python", "app/app.py"]