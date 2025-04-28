# tasks.py
# -----------------------------------------
# Celery-таск для генерации изображения и отправки пользователю
# Исправлено: синхронная отправка через HTTP, одно asyncio.run, обработка ошибок и удаления файла
# -----------------------------------------
import os
import asyncio
import json
import logging
import requests
from celery_app import celery_app
from api import ImageGenerator
from config import API_KEYS, API_TOKEN

logger = logging.getLogger(__name__)

@celery_app.task(
    bind=True,
    name="tasks.generate_image_task",
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3
)
def generate_image_task(self, image_path: str, profession: str, gender: str, user_id: int) -> None:
    """
    Генерирует изображение через AsyncOpenAI и отправляет его пользователю
    синхронным HTTP-запросом к Telegram Bot API.

    Параметры:
      - image_path: путь до исходного фото (в общем томе /shared_tmp)
      - profession: выбранная профессия
      - gender: выбранный пол
      - user_id: Telegram ID пользователя
    """
    # 1. Генерация изображения (одно asyncio.run)
    try:
        async def _gen():
            gen = ImageGenerator(API_KEYS, bot=None)
            return await gen.generate_image(image_path, profession, gender, str(user_id))

        result_path = asyncio.run(_gen())
        if not result_path:
            logger.error("Генерация не вернула результат")
            return
    except Exception as e:
        logger.error(f"Ошибка генерации: {e}")
        # Повторим попытку в случае сбоя генерации
        raise self.retry(exc=e)

    # 2. Отправка изображения через HTTP-запрос
    try:
        url = f"https://api.telegram.org/bot{API_TOKEN}/sendPhoto"
        with open(result_path, 'rb') as f:
            files = {'photo': f}
            # подпись и клавиатура
            caption = "Ваш результат готов!"
            keyboard = {
                'inline_keyboard': [
                    [
                        {'text': 'Помощь', 'callback_data': 'help'},
                        {'text': 'Другую фигурку', 'callback_data': 'another'}
                    ]
                ]
            }
            data = {
                'chat_id': user_id,
                'caption': caption,
                'reply_markup': json.dumps(keyboard)
            }
            resp = requests.post(url, data=data, files=files, timeout=60)
            resp.raise_for_status()
            logger.info(f"Отправлено пользователю {user_id} файл {result_path}")
    except Exception as e:
        logger.error(f"Ошибка отправки в Telegram: {e}")
    finally:
        # 3. Удаление временного файла
        try:
            os.remove(result_path)
            logger.debug(f"Удалён временный файл {result_path}")
        except Exception as e:
            logger.warning(f"Не удалось удалить файл {result_path}: {e}")