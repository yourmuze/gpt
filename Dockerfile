# Dockerfile

FROM python:3.11-slim

# Установка зависимостей системы (если потребуется)
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем только файл зависимостей, чтобы кешировать слой pip install
COPY requirements.txt .

# Устанавливаем Python-зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код вашего проекта
COPY . .

# Гарантируем, что вывод Python не буферизуется
ENV PYTHONUNBUFFERED=1

# По умолчанию никаких команд не задаём,
# управление стартом сервисов отдаём docker-compose
