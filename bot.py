import re
import random
import asyncio
from pathlib import Path
import tempfile
from difflib import SequenceMatcher
from tasks import generate_image_task

import pandas as pd
import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.filters.state import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.enums.chat_action import ChatAction
from aiogram.utils.chat_action import ChatActionSender
from aiogram.exceptions import TelegramNetworkError

from config import (
    API_TOKEN, BASE_DIR, EXCEL_PATH, WAIT_VIDEO_PATH, SUB_CHANNEL_USERNAME,
    ADMIN_CHANNEL_USERNAME, ADMIN_IDS, DB_PATH, API_KEYS, MAX_CONCURRENT_TASKS, logger
)
from api import ImageGenerator

# --------------------
# Инициализация бота
# --------------------
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# Словарь для последних фото
best_file_id: dict[int, str] = {}

# Инициализация генератора изображений
generator = ImageGenerator(API_KEYS, bot)

# --------------------
# Состояния
# --------------------
class Form(StatesGroup):
    check_sub      = State()
    ask_name       = State()
    ask_profession = State()
    choose_gender  = State()
    ask_photo      = State()

# --------------------
# Утилиты
# --------------------
def normalize(text: str) -> str:
    t = text.lower()
    t = re.sub(r"[^\w\s]", "", t)
    return re.sub(r"\s+", " ", t).strip()

# --------------------
# Загрузка профессий
# --------------------
df = pd.read_excel(EXCEL_PATH, sheet_name="Лист1")
raw_professions = df.iloc[3:, 0].dropna().astype(str).tolist()
professions = [normalize(p) for p in raw_professions]

# --------------------
# База данных
# --------------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        # Ожидаемая схема таблицы
        expected_columns = [
            ("user_id", "INTEGER", True),
            ("name", "TEXT", False),
            ("profession", "TEXT", False),
            ("gender", "TEXT", False),
            ("photo_count", "INTEGER", False)
        ]
        
        # Проверяем текущую схему таблицы
        cursor = await db.execute("PRAGMA table_info(users)")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]
        column_types = {col[1]: col[2] for col in columns}
        
        # Проверяем, соответствует ли схема ожидаемой
        needs_migration = False
        if not column_names:
            # Таблица не существует, создаем новую
            await db.execute(
                """
                CREATE TABLE users (
                    user_id     INTEGER PRIMARY KEY,
                    name        TEXT,
                    profession  TEXT,
                    gender      TEXT,
                    photo_count INTEGER DEFAULT 0
                );
                """
            )
            logger.info("Создана новая таблица users")
        else:
            # Проверяем наличие всех столбцов и их типы
            for col_name, col_type, is_pk in expected_columns:
                if col_name not in column_names:
                    await db.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_type}")
                    logger.info(f"Добавлен столбец {col_name} в таблицу users")
                elif column_types[col_name].upper() != col_type.upper():
                    needs_migration = True
                    logger.warning(f"Некорректный тип столбца {col_name}: ожидается {col_type}, найдено {column_types[col_name]}")
            
            # Если схема не соответствует, выполняем миграцию
            if needs_migration or column_names != [col[0] for col in expected_columns]:
                logger.info("Выполняется миграция таблицы users")
                # Создаем временную таблицу с правильной схемой
                await db.execute(
                    """
                    CREATE TABLE users_temp (
                        user_id     INTEGER PRIMARY KEY,
                        name        TEXT,
                        profession  TEXT,
                        gender      TEXT,
                        photo_count INTEGER DEFAULT 0
                    );
                    """
                )
                # Переносим данные, преобразуя типы
                await db.execute(
                    """
                    INSERT INTO users_temp (user_id, name, profession, gender, photo_count)
                    SELECT
                        user_id,
                        name,
                        profession,
                        CASE WHEN gender IN ('male', 'female') THEN gender ELSE NULL END,
                        CASE
                            WHEN photo_count IS NULL THEN 0
                            WHEN photo_count GLOB '[0-9]*' THEN CAST(photo_count AS INTEGER)
                            ELSE 0
                        END
                    FROM users;
                    """
                )
                # Удаляем старую таблицу и переименовываем новую
                await db.execute("DROP TABLE users")
                await db.execute("ALTER TABLE users_temp RENAME TO users")
                logger.info("Миграция таблицы users завершена")
        
        await db.commit()
    logger.info("База данных инициализирована")

async def get_user(uid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id, name, profession, gender, photo_count FROM users WHERE user_id = ?", (uid,))
        user = await cur.fetchone()
        if user:
            user_dict = {
                "user_id": user[0],
                "name": user[1],
                "profession": user[2],
                "gender": user[3],
                "photo_count": user[4]
            }
            logger.debug(f"Получены данные пользователя {uid}: {user_dict}")
            return user_dict
        logger.debug(f"Пользователь {uid} не найден")
        return None

async def upsert_user(uid: int, name=None, profession=None, gender=None, inc_photo=False):
    user = await get_user(uid)
    async with aiosqlite.connect(DB_PATH) as db:
        if user:
            if name:
                await db.execute("UPDATE users SET name = ? WHERE user_id = ?", (name, uid))
                logger.info(f"Обновлено имя пользователя {uid}: {name}")
            if profession:
                await db.execute("UPDATE users SET profession = ? WHERE user_id = ?", (profession, uid))
                logger.info(f"Обновлена профессия пользователя {uid}: {profession}")
            if gender:
                await db.execute("UPDATE users SET gender = ? WHERE user_id = ?", (gender, uid))
                logger.info(f"Обновлен пол пользователя {uid}: {gender}")
            if inc_photo:
                await db.execute("UPDATE users SET photo_count = photo_count + 1 WHERE user_id = ?", (uid,))
                logger.info(f"Увеличен счетчик фото для пользователя {uid}")
        else:
            await db.execute(
                "INSERT INTO users (user_id, name, profession, gender, photo_count) VALUES (?, ?, ?, ?, ?)",
                (uid, name or "", profession or "", gender or "", 1 if inc_photo else 0)
            )
            logger.info(f"Добавлен новый пользователь {uid}: name={name}, profession={profession}, gender={gender}")
        await db.commit()

# --------------------
# Клавиатуры
# --------------------
def sub_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Проверить", callback_data="check_sub"),
        InlineKeyboardButton(text="🔗 Подписаться", url=f"https://t.me/{SUB_CHANNEL_USERNAME.lstrip('@')}" )
    ]])

def retry_prof_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🎲 Случайная профессия", callback_data="random_profession")
    ]])

def gender_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Для него", callback_data="gender_male"),
        InlineKeyboardButton(text="Для нее", callback_data="gender_female")
    ]])

def result_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Помощь", callback_data="help"),
        InlineKeyboardButton(text="Другую фигурку", callback_data="another")
    ]])

# --------------------
# Хэндлеры
# --------------------
@dp.startup()
async def on_startup():
    global ADMIN_CHAT_ID
    await init_db()
    chat = await bot.get_chat(ADMIN_CHANNEL_USERNAME)
    ADMIN_CHAT_ID = chat.id
    for _ in range(MAX_CONCURRENT_TASKS):
        asyncio.create_task(generator.worker())
    logger.info("Бот запущен, воркеры генератора изображений активированы")

@dp.message(CommandStart())
async def cmd_start(msg: types.Message, state: FSMContext):
    await msg.answer(
        f"Чтобы получить аватарку, подпишитесь на {SUB_CHANNEL_USERNAME}",
        reply_markup=sub_keyboard()
    )
    await state.set_state(Form.check_sub)
    logger.info(f"Пользователь {msg.from_user.id} начал взаимодействие с ботом")

@dp.callback_query(StateFilter(Form.check_sub), F.data == "check_sub")
async def check_sub(call: types.CallbackQuery, state: FSMContext):
    try:
        m = await bot.get_chat_member(SUB_CHANNEL_USERNAME, call.from_user.id)
        ok = m.status in ("creator", "administrator", "member")
    except Exception as e:
        ok = False
        logger.error(f"Ошибка проверки подписки для user_id={call.from_user.id}: {e}")
    if ok:
        await call.message.edit_text(
            "Напишите, как вас зовут. Лучше оставить только имя, чтобы результат был без ошибок"
        )
        await state.set_state(Form.ask_name)
        logger.info(f"Пользователь {call.from_user.id} подписан, перешел к вводу имени")
    else:
        await call.message.edit_text(
            f"Вы не подписаны, просьба перейти и подписаться на {SUB_CHANNEL_USERNAME}",
            reply_markup=sub_keyboard()
        )
        logger.warning(f"Пользователь {call.from_user.id} не подписан")

@dp.message(StateFilter(Form.ask_name))
async def process_name(msg: types.Message, state: FSMContext):
    await upsert_user(msg.from_user.id, name=msg.text.strip())
    await msg.answer("Укажите вашу профессию")
    await state.set_state(Form.ask_profession)
    logger.info(f"Пользователь {msg.from_user.id} ввел имя: {msg.text.strip()}")

@dp.message(StateFilter(Form.ask_profession))
async def process_profession(msg: types.Message, state: FSMContext):
    text, best, score = normalize(msg.text), None, 0.0
    for p in professions:
        s = SequenceMatcher(None, text, p).ratio()
        if s > score:
            best, score = p, s
    if score >= 0.75:
        await upsert_user(msg.from_user.id, profession=best)
        await msg.answer(
            "Выберите, для кого создаем результат", reply_markup=gender_keyboard()
        )
        await state.set_state(Form.choose_gender)
        logger.info(f"Пользователь {msg.from_user.id} выбрал профессию: {best}")
    else:
        await msg.answer(
            "К сожалению, такой профессии нет.\n\nПопробуйте изменить написание или согласитесь на случайный вариант.",
            reply_markup=retry_prof_keyboard()
        )
        logger.warning(f"Пользователь {msg.from_user.id} ввел неверную профессию: {msg.text}")

@dp.callback_query(F.data == "random_profession")
async def random_prof(call: types.CallbackQuery, state: FSMContext):
    prof = random.choice(professions)
    await upsert_user(call.from_user.id, profession=prof)
    await call.message.edit_text(f"Ваша профессия — {prof}")
    await call.message.answer(
        "Выберите, для кого создаем результат", reply_markup=gender_keyboard()
    )
    await state.set_state(Form.choose_gender)
    logger.info(f"Пользователь {call.from_user.id} получил случайную профессию: {prof}")

@dp.callback_query(F.data.in_(["gender_male", "gender_female"]))
async def choose_gender(call: types.CallbackQuery, state: FSMContext):
    gender = "male" if call.data == "gender_male" else "female"
    await upsert_user(call.from_user.id, gender=gender)
    await call.message.delete()
    await call.message.answer("Чтобы получить фигурку, пришлите свое фото в чат")
    await state.set_state(Form.ask_photo)
    logger.info(f"Пользователь {call.from_user.id} выбрал пол: {gender}")

@dp.message(StateFilter(Form.ask_photo), ~F.photo)
async def not_photo(msg: types.Message):
    await msg.answer("Пожалуйста, пришлите фото в медиа-формате")
    logger.warning(f"Пользователь {msg.from_user.id} отправил не фото")

@dp.message(StateFilter(Form.ask_photo), F.photo)
async def process_photo(msg: types.Message, state: FSMContext):
    # Проверяем лимит фотографий
    user = await get_user(msg.from_user.id)
    photo_count = int(user["photo_count"]) if user and user["photo_count"] is not None else 0
    if photo_count >= 2:
        await msg.answer("Вы достигли лимита фото (2)")
        logger.info(f"Пользователь {msg.from_user.id} достиг лимита фото")
        return

    # Увеличиваем счётчик и сообщаем пользователю
    await upsert_user(msg.from_user.id, inc_photo=True)
    await msg.answer("Создаем вашу фигурку\n\nЭто займет время.")
    logger.info(f"Пользователь {msg.from_user.id} отправил фото, обработка начата")

    # Скачиваем фото во временный файл
    photo = msg.photo[-1]
    file = await bot.get_file(photo.file_id)
    with tempfile.NamedTemporaryFile(dir="/shared_tmp", delete=False, suffix=".jpg") as tmp:
        await bot.download_file(file.file_path, tmp.name)
        image_path = tmp.name
    logger.debug(f"Фото пользователя {msg.from_user.id} скачано: {image_path}")

    # Отправляем заглушку видео ровно один раз
    try:
        await bot.send_chat_action(chat_id=msg.chat.id, action=ChatAction.UPLOAD_VIDEO)
        await msg.answer_video(
            FSInputFile(str(WAIT_VIDEO_PATH)),
            supports_streaming=True
        )
        logger.info(f"Видео-заглушка отправлена пользователю {msg.from_user.id}")
    except Exception as e:
        logger.error(f"Не удалось отправить видео-заглушку: {e}")

    # Ставим одну задачу в очередь на генерацию изображения
    user_data = await get_user(msg.from_user.id)
    profession = user_data["profession"]
    gender     = user_data["gender"]
    generate_image_task.delay(image_path, profession, gender, msg.from_user.id)
    await msg.answer("Ваша задача в очереди, ждите результата…")

    # Очищаем состояние
    await state.clear()


@dp.callback_query(F.data == "help")
async def help_req(call: types.CallbackQuery):
    uid = call.from_user.id
    await bot.forward_message(
        chat_id=ADMIN_CHAT_ID,
        from_chat_id=call.message.chat.id,
        message_id=call.message.message_id
    )
    await bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=f"HELP запрос от пользователя {uid}"
    )
    await call.answer(
        "Мы проверим результат и вернемся с исправленным, если найдем ошибку", show_alert=True
    )
    logger.info(f"Пользователь {uid} запросил помощь")

@dp.callback_query(F.data == "another")
async def another_fun(call: types.CallbackQuery, state: FSMContext):
    await call.message.edit_reply_markup(reply_markup=None)
    await call.message.answer("Пришлите новое фото для фигурки")
    await state.set_state(Form.ask_photo)
    logger.info(f"Пользователь {call.from_user.id} запросил другую фигурку")

# --------------------
# Админские команды (из чата)
# --------------------
@dp.message(Command("broadcast"))
async def admin_broadcast(msg: types.Message):
    if msg.from_user.id not in ADMIN_IDS:
        await msg.reply("❌ У вас нет прав для этой команды.")
        logger.warning(f"Пользователь {msg.from_user.id} попытался выполнить /broadcast без прав.")
        return
    parts = msg.text.split(' ', 1)
    if len(parts) < 2:
        await msg.reply("❌ Укажите текст: /broadcast <текст>")
        return
    text = parts[1]
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id FROM users")
        ids = [r[0] for r in await cur.fetchall()]
    success = 0
    for uid in ids:
        try:
            await bot.send_message(chat_id=uid, text=text)
            success += 1
        except Exception as e:
            logger.warning(f"Не удалось отправить сообщение пользователю {uid}: {e}")
    await msg.reply(f"✅ Рассылка выполнена: {success} пользователей.")
    logger.info(f"Рассылка выполнена: {success} пользователей.")

@dp.message(Command("send"))
async def admin_send(msg: types.Message):
    if msg.from_user.id not in ADMIN_IDS:
        await msg.reply("❌ У вас нет прав для этой команды.")
        logger.warning(f"Пользователь {msg.from_user.id} попытался выполнить /send без прав.")
        return
    parts = msg.text.split(' ', 2)
    if len(parts) < 3:
        await msg.reply("❌ Использование: /send <user_id> <текст>")
        return
    try:
        uid = int(parts[1])
    except ValueError:
        await msg.reply("❌ User ID должен быть числом.")
        return
    text = parts[2]
    try:
        await bot.send_message(chat_id=uid, text=text)
        await msg.reply(f"✅ Сообщение отправлено пользователю {uid}.")
        logger.info(f"Сообщение отправлено пользователю {uid}.")
    except Exception as e:
        await msg.reply(f"❌ Не удалось отправить: {e}")
        logger.error(f"Не удалось отправить сообщение пользователю {uid}: {e}")

@dp.message(Command("reset"))
async def admin_reset(msg: types.Message):
    if msg.from_user.id not in ADMIN_IDS:
        await msg.reply("❌ У вас нет прав для этой команды.")
        logger.warning(f"Пользователь {msg.from_user.id} попытался выполнить /reset без прав.")
        return
    parts = msg.text.split(' ', 1)
    if len(parts) < 2:
        await msg.reply("❌ Использование: /reset <user_id>")
        return
    try:
        uid = int(parts[1])
    except ValueError:
        await msg.reply("❌ User ID должен быть числом.")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET photo_count = 0 WHERE user_id = ?", (uid,))
        await db.commit()
    await msg.reply(f"✅ Счетчик фото для пользователя {uid} сброшен.")
    logger.info(f"Счетчик фото сброшен для пользователя {uid}.")

# --------------------
# Админские команды (из канала)
# --------------------
@dp.channel_post(Command("broadcast"))
async def channel_broadcast(post: types.Message):
    if post.chat.id != ADMIN_CHAT_ID:
        logger.warning(f"Попытка выполнить /broadcast из неверного канала: {post.chat.id}")
        return
    parts = post.text.split(' ', 1)
    if len(parts) < 2:
        logger.warning("Команда /broadcast в канале без текста.")
        return
    text = parts[1]
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id FROM users")
        ids = [r[0] for r in await cur.fetchall()]
    success = 0
    for uid in ids:
        try:
            await bot.send_message(chat_id=uid, text=text)
            success += 1
        except Exception as e:
            logger.warning(f"Не удалось отправить сообщение пользователю {uid}: {e}")
    logger.info(f"Рассылка из канала выполнена: {success} пользователей.")

@dp.channel_post(Command("send"))
async def channel_send(post: types.Message):
    if post.chat.id != ADMIN_CHAT_ID:
        logger.warning(f"Попытка выполнить /send из неверного канала: {post.chat.id}")
        return
    parts = post.text.split(' ', 2)
    if len(parts) < 3:
        logger.warning("Команда /send в канале без user_id или текста.")
        return
    try:
        uid = int(parts[1])
    except ValueError:
        logger.warning("Команда /send в канале с некорректным user_id.")
        return
    text = parts[2]
    try:
        await bot.send_message(chat_id=uid, text=text)
        logger.info(f"Сообщение отправлено пользователю {uid} из канала.")
    except Exception as e:
        logger.error(f"Не удалось отправить сообщение пользователю {uid} из канала: {e}")

@dp.channel_post(Command("reset"))
async def channel_reset(post: types.Message):
    if post.chat.id != ADMIN_CHAT_ID:
        logger.warning(f"Попытка выполнить /reset из неверного канала: {post.chat.id}")
        return
    parts = post.text.split(' ', 1)
    if len(parts) < 2:
        logger.warning("Команда /reset в канале без user_id.")
        return
    try:
        uid = int(parts[1])
    except ValueError:
        logger.warning("Команда /reset в канале с некорректным user_id.")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET photo_count = 0 WHERE user_id = ?", (uid,))
        await db.commit()
    logger.info(f"Счетчик фото сброшен для пользователя {uid} из канала.")

if __name__ == "__main__":
    dp.run_polling(bot, skip_updates=True)