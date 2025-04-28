# api.py

import asyncio
import os
import base64
import tempfile

import aiosqlite
from openai import AsyncOpenAI
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile

from config import (
    API_KEYS,
    DB_PATH,
    OUTPUT_DIR,
    DELAY_BETWEEN_REQUESTS,
    logger
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
        await self.lock.acquire()
        try:
            key = self.api_keys[self.current_key_index]
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            logger.debug(f"Selected API key: {key[:5]}…")
            return key
        finally:
            self.lock.release()

    async def generate_image(self, image_path: str, profession: str, gender: str, user_id: str) -> str | None:
        prompt = f"Transform this person into a {profession}, {gender} in a anime style."
        api_key = await self.get_next_api_key()
        client = AsyncOpenAI(api_key=api_key)

        if not os.path.exists(image_path):
            logger.error(f"Image not found: {image_path} for user {user_id}")
            return None

        try:
            with open(image_path, "rb") as image_file:
                response = await client.images.edit(
                model="gpt-image-1",
                image=image_file,
                prompt=prompt,
                n=1,
                size="1024x1024",
                quality="medium"         
    )

            # Always returns b64_json for gpt-image-1
            image_base64 = response.data[0].b64_json
            if not image_base64:
                logger.error(f"No image data returned for user {user_id}")
                return None

            image_bytes = base64.b64decode(image_base64)
            filename = f"result_{user_id}_{int(asyncio.get_event_loop().time())}.png"
            output_path = os.path.join(OUTPUT_DIR, filename)

            with open(output_path, "wb") as out_file:
                out_file.write(image_bytes)

            logger.info(f"Image saved: {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"Generation error for user {user_id}: {e}")
            return None

    async def worker(self):
        while True:
            try:
                image_path, profession, gender, user_id = await self.queue.get()
                result_path = await self.generate_image(image_path, profession, gender, user_id)

                if result_path:
                    # Fetch current photo_count to determine caption
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
                            "Можете написать /help, если в генерации возникла ошибка с написанием"
                        )
                    else:
                        caption = "Ваш результат готов!"

                    await self.bot.send_photo(
                        chat_id=user_id,
                        photo=FSInputFile(result_path),
                        caption=caption,
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [
                                InlineKeyboardButton(text="Помощь", callback_data="help"),
                                InlineKeyboardButton(text="Другую фигурку", callback_data="another")
                            ]
                        ])
                    )
                    os.remove(result_path)
                    logger.info(f"Sent image to {user_id}, removed file {result_path}")
                else:
                    await self.bot.send_message(
                        chat_id=user_id,
                        text="Ошибка генерации изображения."
                    )
                    logger.warning(f"Failed to generate image for user {user_id}")

                self.queue.task_done()
                await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

            except Exception as e:
                logger.error(f"Worker error: {e}")
                self.queue.task_done()

    async def add_task(self, image_path: str, profession: str, gender: str, user_id: str):
        await self.queue.put((image_path, profession, gender, user_id))
        logger.info(f"Task added for user {user_id}: profession={profession}, gender={gender}")
