# Базовый образ с Python 3.10
FROM python:3.10-slim

# Устанавливаем системные зависимости (если понадобятся)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Создаём рабочую директорию
WORKDIR /app

# Копируем список зависимостей и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь исходный код внутрь контейнера
COPY . .

# По умолчанию пусть контейнер запускает бота.
# Для воркера будет переопределено в docker-compose.yml
CMD ["python", "bot.py"]
