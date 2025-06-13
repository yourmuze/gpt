import os
import ast
import logging

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Загружаем из .env
API_TOKEN = os.getenv("API_TOKEN")
SUB_CHANNEL_USERNAME = os.getenv("SUB_CHANNEL_USERNAME", "").lstrip("@")
ADMIN_CHANNEL_USERNAME = os.getenv("ADMIN_CHANNEL_USERNAME", "").lstrip("@")
# Ожидаем строку вида "[434092620, 386406595]"
ADMIN_IDS = ast.literal_eval(os.getenv("ADMIN_IDS", "[]"))

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

@dp.message(Command("send"))
async def send_message_to_user(msg: types.Message):
    """
    /send <user_id> <text>
    - в личке бот: только для user_id из ADMIN_IDS
    - в канале: только для чата с username=ADMIN_CHANNEL_USERNAME
    """
    chat_type = msg.chat.type  # строка: "private", "group", "supergroup", "channel"
    # Проверяем источник команды
    if chat_type == 'private':
        if msg.from_user.id not in ADMIN_IDS:
            logger.warning(f"Неавторизованный юзер {msg.from_user.id} попытался /send в личке")
            return
    elif chat_type == 'channel':
        if msg.chat.username != ADMIN_CHANNEL_USERNAME:
            logger.warning(f"Попытка /send из канала @{msg.chat.username} вместо @{ADMIN_CHANNEL_USERNAME}")
            return
    else:
        # команды из групп/супергрупп запрещены
        return

    parts = msg.text.split(" ", 2)
    if len(parts) < 3:
        await msg.answer("❗️ Usage: /send <user_id> <text>")
        logger.warning("Команда /send без аргументов")
        return

    try:
        target_id = int(parts[1])
    except ValueError:
        await msg.answer("❗️ Некорректный user_id, ожидается число.")
        logger.warning(f"Неверный user_id в /send: {parts[1]}")
        return

    text = parts[2]
    try:
        await bot.send_message(chat_id=target_id, text=text)
        confirmation = f"✅ Сообщение отправлено пользователю {target_id}"
        if chat_type == 'private':
            await msg.answer(confirmation)
        logger.info(confirmation)
    except Exception as e:
        error_msg = f"❌ Не удалось отправить пользователю {target_id}: {e}"
        if chat_type == 'private':
            await msg.answer(error_msg)
        logger.error(error_msg)

@dp.message()
async def stub_handler(msg: types.Message):
    """Отвечает на любые сообщения одним текстом, кроме команды /send"""
    if msg.text and msg.text.split()[0].lower() == '/send':
        return

    reply_text = "📢 Конкурс завершён. Команда hh.ru благодарит вас за участие!"
    await msg.answer(reply_text)
    logger.info(f"Stub reply sent to {msg.from_user.id}")

if __name__ == "__main__":
    # skip_updates=True, чтобы не обрабатывать старые апдейты
    dp.run_polling(bot, skip_updates=True)
