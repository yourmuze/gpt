from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from config import TELEGRAM_TOKEN, ADMIN_CHAT_ID
from tasks import generate_image
from sessions import session_manager
from io import BytesIO
import aiohttp

# Инициализация бота
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

@dp.message_handler(content_types=['photo'])
async def handle_photo(message: types.Message):
    """Обработка фото с подписью от пользователя."""
    photo = message.photo[-1]  # Берем фото максимального качества
    caption = message.caption or ""  # Подпись, если есть

    # Скачивание фото в BytesIO
    bio = BytesIO()
    await photo.download(destination_file=bio)
    bio.seek(0)

    # Запуск задачи генерации изображения
    generate_image.delay(
        bio.getvalue(),  # image_bytes
        caption,
        "default_style",
        "default_palette",
        message.chat.id,
        message.message_id
    )
    await message.reply("Ваш запрос принят, ждите результат!")

@dp.message_handler(commands=['sessions'])
async def handle_sessions(message: types.Message):
    """Команда для проверки статуса сессий (только для админа)."""
    if message.chat.id != ADMIN_CHAT_ID:
        return
    sessions_status = "\n".join([f"Session {s.id}: {s.status}" for s in session_manager.sessions])
    await message.reply(f"Статус сессий:\n{sessions_status}")

@dp.message_handler(commands=['restart'])
async def handle_restart(message: types.Message):
    """Команда для перезапуска сессии по ID (только для админа)."""
    if message.chat.id != ADMIN_CHAT_ID:
        return
    try:
        session_id = int(message.text.split()[1])
        session = next(s for s in session_manager.sessions if s.id == session_id)
        session.stop()
        session.start()
        await message.reply(f"Сессия {session_id} перезапущена")
    except Exception as e:
        await message.reply(f"Ошибка при перезапуске сессии: {str(e)}")