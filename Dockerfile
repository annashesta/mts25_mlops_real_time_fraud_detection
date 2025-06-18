# Этап 2: Основной образ
FROM python:3.12-slim
WORKDIR /app

# Копируем большие файлы напрямую из локальной директории
COPY model/catboost_model.cbm /app/model/
COPY model/threshold.json /app/model/
COPY model/categorical_features.json /app/model/

# Копируем остальные файлы напрямую
COPY requirements.txt .
COPY config.yaml .
COPY src/ /app/src/
COPY app/ /app/app/
COPY templates/ /app/templates/

# Установка зависимостей
RUN pip install --no-cache-dir -r requirements.txt && \
    rm -rf /root/.cache

# Настройка прав
RUN mkdir -p /app/input /app/output /app/logs && \
    useradd -m appuser && \
    chown -R appuser:appuser /app && \
    chmod -R 755 /app/logs

USER appuser

CMD ["sh", "-c", "exec \"$@\"", "$0"]