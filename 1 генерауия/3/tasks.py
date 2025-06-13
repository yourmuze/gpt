# tasks.py

import os
import json
import base64
import random
import logging
import pandas as pd
import re
from config import ACCESSORIES_FILE, PACKAGING_PROMPT_TEMPLATE
import openai
from openai import RateLimitError
import requests
from celery_app import celery_app
from config import API_KEYS, API_TOKEN, DB_PATH, REDIS_URL, REF_MALE, REF_FEMALE
import sqlite3
import redis

logger = logging.getLogger(__name__)

_r = redis.Redis.from_url(REDIS_URL)
def pick_api_key() -> str:
    # atomically incr counter and mod by keys count
    idx = int(_r.incr("api_key_pointer")) % len(API_KEYS)
    logger.info(f"pick_api_key: using key index={idx}")
    return API_KEYS[idx]

def _norm(s: str) -> str:
    t = s.lower()
    t = re.sub(r"[^\w\s]", "", t)
    return re.sub(r"\s+", " ", t).strip()

# загружаем маппинг профессия → список аксессуаров
_acc_df = pd.read_excel(ACCESSORIES_FILE)
_acc_df.columns = _acc_df.columns.str.strip()
_acc_df["ПРОФЕССИЯ"] = _acc_df["ПРОФЕССИЯ"] \
    .astype(str) \
    .str.replace("/", ",", regex=False)
_acc_df = _acc_df.assign(
    ПРОФЕССИЯ=_acc_df["ПРОФЕССИЯ"].str.split(",")
).explode("ПРОФЕССИЯ")
_acc_df["ПРОФЕССИЯ"] = _acc_df["ПРОФЕССИЯ"].str.strip()

# 2) Теперь строим mapping, дублируя аксессуары
_accessories_map: dict[str, list[str]] = {}
for _, row in _acc_df.iterrows():
    prof = _norm(row["ПРОФЕССИЯ"])
    items: list[str] = []
    for col in _acc_df.columns:
        if col.startswith("Аксессуар_") and isinstance(row[col], str) and row[col].strip():
            items.append(row[col].strip())
    # если одна и та же профессия встречалась несколько раз, последний overwrite дублирует список
    _accessories_map[prof] = items

@celery_app.task(
    bind=True,
    name="tasks.generate_image_task",
    autoretry_for=(RateLimitError,),
    retry_backoff=True,
    max_retries=5
)
def generate_image_task(self, image_path: str, profession: str, gender: str, user_id: int) -> None:
    """
    Celery-таск: синхронно генерирует изображение по исходному фото и отправляет его пользователю.

    Аргументы:
      - image_path: путь до временного файла с фото пользователя
      - profession: профессия
      - gender: пол ("male"/"female")
      - user_id: Telegram ID
    """
    api_key = pick_api_key()
    openai.api_key = api_key
    
    norm_prof = _norm(profession)
    acc_list = _accessories_map.get(norm_prof, [])

    # выбираем ровно 6 штук (с повторениями, если мало)
    if len(acc_list) >= 6:
        selected = random.sample(acc_list, 6)
    else:
        selected = random.choices(acc_list, k=6)

    # собираем окончательный prompt
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT name FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    user_name = row[0] if row and row[0] else "Пользователь"

    # Подставляем все переменные в шаблон
    full_prompt = PACKAGING_PROMPT_TEMPLATE.format(
        profession=profession,
        accessories=", ".join(selected),
        name=user_name
    )
    
    ref_path = REF_MALE if gender == "male" else REF_FEMALE

    # 2. Запрос к OpenAI Image Edit
    try:
        with open(image_path, "rb") as selfie, open(ref_path, "rb") as ref:
            response = openai.images.edit(
                model="gpt-image-1",
                image=[selfie, ref],       # <-- здесь список файлов
                prompt=full_prompt,
                n=1,
                size="1024x1024",
                quality="medium"
            )
        
    except RateLimitError as e:
        logger.warning(f"[{user_id}] Rate limit exceeded, retrying: {e}")
        # автоматически retry по декоратору
        raise
    except Exception as e:
        logger.error(f"[{user_id}] Ошибка генерации изображения: {e}")
        # если хотим ретраиться и на другие ошибки, можно раскинуть сюда
        raise self.retry(exc=e)

    # 3. Декодируем Base64 и сохраняем результат
    try:
        image_obj = response.data[0]
        b64 = image_obj.b64_json
        img_bytes = base64.b64decode(b64)
    except Exception as e:
        logger.error(f"[{user_id}] Некорректный ответ от OpenAI: {e}")
        raise self.retry(exc=e)

    result_path = f"{os.path.splitext(image_path)[0]}_result.png"
    try:
        with open(result_path, "wb") as out_f:
            out_f.write(img_bytes)
        logger.info(f"[{user_id}] Сохранено изображение: {result_path}")
    except Exception as e:
        logger.error(f"[{user_id}] Не удалось сохранить файл: {e}")
        raise self.retry(exc=e)

    # 4. Отправляем в Telegram через HTTP (requests)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT photo_count FROM users WHERE user_id = ?",
        (user_id,)
    )
    row = cur.fetchone()
    conn.close()
    count = row[0] if row else 0

    url = f"https://api.telegram.org/bot{API_TOKEN}/sendPhoto"

    if count > 1:
        caption = (
            "Большое спасибо, что поучаствовали!❤️\n\n"
            "Вы использовали все доступные попытки.\n\n"
            "Обязательно ставьте фигурку на аватарку и не меняйте её до окончания акции и объявления победителей — 5 июня! 🤞\n\n"
            "А если вам понравился результат, поделитесь им и ссылкой на бота с близкими — вдруг они тоже коллекционируют классный мерч.\n\n"
            "Если что-то пошло не так, жмите /help 🥺"
        ) 
            # 4. Отправляем в Telegram через HTTP (requests)
    # --- готовим caption и reply_markup в зависимости от номера попытки ---
    if count > 1:
        caption = (
            "Большое спасибо, что поучаствовали!❤️\n\n"
            "Вы использовали все доступные попытки.\n\n"
            "Обязательно ставьте фигурку на аватарку и не меняйте её до окончания акции и объявления победителей — 5 июня! 🤞\n\n"
            "А если вам понравился результат, поделитесь им и ссылкой на бота с близкими — вдруг они тоже коллекционируют классный мерч.\n\n"
            "Если что-то пошло не так, жмите /help 🥺"
        )
        reply_markup = None
    else:
        # первая попытка — даём кнопку «Другую фигурку»
        caption = (
            "Ваша фигурка готова 🥳 Скорее скачивайте, ставьте на аватарку в Telegram и не меняйте до конца конкурса — 5 июня!\n\n"
            "И не забудьте поделиться с друзьями, пусть тоже поучаствуют в розыгрыше приза!\n\n"
            "Если вдруг что-то не так, пишите /help 🥺"
        )
        reply_markup = json.dumps({
            "inline_keyboard": [[
                {"text": "Другую фигурку", "callback_data": "another"}
            ]]
        })

    data = {
        "chat_id": user_id,
        "caption": caption,
    }
    if reply_markup:
        data["reply_markup"] = reply_markup

    try:
        with open(result_path, "rb") as photo_f:
            resp = requests.post(
                url,
                data=data,
                files={"photo": photo_f},
                timeout=60
            )
        resp.raise_for_status()
        logger.info(f"[{user_id}] Изображение успешно отправлено")

        # Обновляем last_photo_id в базе
        try:
            result = resp.json().get("result", {})
            photo_msg_id = result.get("message_id")
            if photo_msg_id is not None:
                conn = sqlite3.connect(DB_PATH, timeout=10)
                cur = conn.cursor()
                cur.execute(
                    "UPDATE users SET last_photo_id = ? WHERE user_id = ?;",
                    (photo_msg_id, user_id)
                )
                conn.commit()
                conn.close()
                logger.debug(f"[{user_id}] last_photo_id обновлён: {photo_msg_id}")
        except Exception as e:
            logger.warning(f"[{user_id}] Не удалось сохранить last_photo_id: {e}")

    except Exception as e:
        logger.error(f"[{user_id}] Ошибка отправки в Telegram: {e}")

    finally:
        # 5. Чистим временные файлы
        for path in (image_path, result_path):
            try:
                os.remove(path)
                logger.debug(f"[{user_id}] Удалён файл: {path}")
            except Exception as ex:
                logger.warning(f"[{user_id}] Не удалось удалить файл {path}: {ex}")
