# api.py

import asyncio
import os
import base64
import re

import aiosqlite
from openai import AsyncOpenAI, RateLimitError
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile

from config import (
    API_KEYS,
    DB_PATH,
    OUTPUT_DIR,
    DELAY_BETWEEN_REQUESTS,
    logger,
    REF_MALE,
    REF_FEMALE
)

class ImageGenerator:
    def __init__(self, api_keys, bot):
        self.api_keys = api_keys
        self.current_key_index = 0
        self.queue = asyncio.Queue()
        self.lock = asyncio.Lock()
        self.bot = bot
        logger.debug(f"Initialized lock: {type(self.lock)}")

    async def get_next_api_key(self) -> str:
        async with self.lock:
            key = self.api_keys[self.current_key_index]
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            logger.debug(f"Selected API key: {key[:5]}…")
            return key

    async def generate_image(
        self,
        image_path: str,
        profession: str,
        gender: str,
        user_id: str
    ) -> str:
        if not os.path.exists(image_path):
            msg = f"Image not found: {image_path} for user {user_id}"
            logger.error(msg)
            raise FileNotFoundError(msg)

        ref_path = REF_MALE if gender == "male" else REF_FEMALE

        prompt = f"Transform this person into a {profession}, {gender} in a realistic style."
        api_key = await self.get_next_api_key()
        client = AsyncOpenAI(api_key=api_key)

        try:
            with open(image_path, "rb") as img_file, open(ref_path, "rb") as ref_file:
                response = await client.images.edit(
                    model="gpt-image-1",
                    prompt=prompt,
                    image=[img_file, ref_file],
                    n=1,
                    size="1024x1024",
                    quality="medium"
                )
            b64 = response.data[0].b64_json
            if not b64:
                raise RuntimeError("Empty image data from OpenAI")

            data = base64.b64decode(b64)
            filename = f"result_{user_id}_{int(asyncio.get_event_loop().time())}.png"
            output_path = os.path.join(OUTPUT_DIR, filename)
            with open(output_path, "wb") as out_file:
                out_file.write(data)

            logger.info(f"Image saved: {output_path}")
            return output_path

        except RateLimitError as e:
            retry_after = None
            if hasattr(e, 'headers') and e.headers.get('Retry-After'):
                retry_after = float(e.headers['Retry-After'])
            else:
                m = re.search(r"after ([0-9]+(?:\.[0-9]+)?) seconds", str(e))
                retry_after = float(m.group(1)) if m else DELAY_BETWEEN_REQUESTS
            logger.warning(f"Rate limit exceeded, sleeping {retry_after}s for user {user_id}")
            await asyncio.sleep(retry_after)
            return await self.generate_image(image_path, profession, gender, user_id)

        except Exception as e:
            logger.error(f"Generation error for user {user_id}: {e}")
            raise

    async def worker(self):
        while True:
            image_path, profession, gender, user_id = await self.queue.get()
            try:
                result_path = await self.generate_image(image_path, profession, gender, user_id)

                async with aiosqlite.connect(DB_PATH) as db:
                    cur = await db.execute(
                        "SELECT photo_count FROM users WHERE user_id = ?",
                        (int(user_id),)
                    )
                    row = await cur.fetchone()
                    count = row[0] if row else 0

                if count > 1:
                    caption = (
                        "Возвращаемся с результатом. Можете скачать, поделиться\n\n"
                        "Большое спасибо за участие! Попытки закончились\n"
                        "Можете нажать help, если в генерации возникла ошибка с написанием"
                        )
                else:
                    caption = "Ваша фигурка готова 🥳 Скорее скачивайте, ставьте на аватарку в Telegram и не меняйте до конца конкурса — 5 июня!\nИ не забудьте поделиться с друзьями, пусть тоже поучаствуют в розыгрыше приза!\nЕсли вдруг что-то не так, нажмите help🥺"

                message = await self.bot.send_photo(
                        chat_id=user_id,
                        photo=FSInputFile(result_path),
                        caption=caption,
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="help", callback_data="help"),
                        InlineKeyboardButton(text="Другую фигурку", callback_data="another")
                    ]])
                )

                from bot import best_file_id
                best_file_id[user_id] = message.message_id

                os.remove(result_path)
                logger.info(f"Sent image to {user_id}, removed file {result_path}")

            except Exception as e:
                logger.error(f"Worker error: {e}")

            finally:
                self.queue.task_done()
                await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

    async def add_task(self, image_path: str, profession: str, gender: str, user_id: str):
        await self.queue.put((image_path, profession, gender, user_id))
        logger.info(f"Task added for user {user_id}: profession={profession}, gender={gender}")
