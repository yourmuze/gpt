# Проект генерации изображений с использованием DALL·E

## Описание
Проект представляет собой систему для генерации изображений на основе текстовых описаний с использованием DALL·E. Система построена на базе Selenium Grid, Celery, Redis и aiogram для взаимодействия с Telegram.

## Требования
- Docker
- Docker-Compose

## Установка и запуск

### 1. Клонирование репозитория
```bash
git clone <repository_url>
cd <project_directory>
```

### 2. Создание файла `.env`
Создайте файл `.env` в корне проекта со следующими переменными:
```
TELEGRAM_TOKEN=your_telegram_bot_token_here
ADMIN_CHAT_ID=your_admin_chat_id_here
REDIS_URL=redis://redis:6379/0
SELENIUM_HUB_URL=http://selenium-hub:4444/wd/hub
PROFILES_DIR=/path/to/profiles/on/host
NUM_SESSIONS=20
```

### 3. Запуск для локальной отладки (с 3 нодами)
```bash
docker-compose up -d --scale chrome-node=3
```

### 4. Запуск для продакшн-деплоя (с 20 нодами)
```bash
docker-compose up -d --scale chrome-node=20
```

### 5. Управление
- Перезапуск стека:
  ```bash
  docker-compose restart
  ```
- Обновление кода:
  ```bash
  git pull && docker-compose build && docker-compose up -d
  ```
- Просмотр логов:
  ```bash
  docker-compose logs -f bot
  docker-compose logs -f chrome-node_1
  ```

## Переменные окружения
- `TELEGRAM_TOKEN`: Токен вашего Telegram-бота.
- `ADMIN_CHAT_ID`: ID администратора в Telegram.
- `REDIS_URL`: URL для подключения к Redis.
- `SELENIUM_HUB_URL`: URL Selenium Hub.
- `PROFILES_DIR`: Путь к папке с профилями на хосте.
- `NUM_SESSIONS`: Количество сессий (нод) Selenium.

## Пользовательский поток
1. Пользователь отправляет фото с подписью в Telegram-бот.
2. Бот принимает запрос и отправляет задачу на генерацию изображения в Celery.
3. Celery-воркер обрабатывает задачу, используя Selenium для взаимодействия с DALL·E.
4. После генерации изображение отправляется пользователю в Telegram.

## Админские команды
- `/sessions`: Показывает статус всех сессий.
- `/restart <id>`: Перезапускает сессию с указанным ID.