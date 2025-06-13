import re
import random
import asyncio
from pathlib import Path
import tempfile
from difflib import SequenceMatcher
from tasks import generate_image_task
from config import ACCESSORIES_FILE, STOP_NAME_WORDS

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
    API_TOKEN, BASE_DIR, WAIT_VIDEO_PATH, SUB_CHANNEL_USERNAME,
    ADMIN_CHANNEL_USERNAME, ADMIN_IDS, DB_PATH, API_KEYS, MAX_CONCURRENT_TASKS, logger
)
from api import ImageGenerator
from typing import Set, Tuple
from celery_app import celery_app
import sqlite3
from aiogram.exceptions import TelegramForbiddenError, TelegramNotFound

processed_media_groups: Set[Tuple[int, str]] = set()

# --------------------
# Инициализация бота
# --------------------
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

best_file_id: dict[int, int] = {}

# Проксируем только send_photo, чтобы сохранять последнее фото
_orig_send_photo = bot.send_photo
async def _send_photo_recorder(chat_id: int, *args, **kwargs):
    msg = await _orig_send_photo(chat_id=chat_id, *args, **kwargs)
    best_file_id[chat_id] = msg.message_id
    return msg
bot.send_photo = _send_photo_recorder  # type: ignore

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

disable_web_page_preview=True

# --------------------
# Загрузка профессий
# --------------------

df = pd.read_excel(ACCESSORIES_FILE)
df.columns = df.columns.str.strip()                         # убираем лишние пробелы в названиях столбцов
df["ПРОФЕССИЯ"] = df["ПРОФЕССИЯ"] \
    .astype(str) \
    .str.replace("/", ",", regex=False)                     # приводим слэши к запятым
df = df.assign(
    ПРОФЕССИЯ=df["ПРОФЕССИЯ"].str.split(",")               # разбиваем по запятой
).explode("ПРОФЕССИЯ")                                      # «взрываем» строки
df["ПРОФЕССИЯ"] = df["ПРОФЕССИЯ"].str.strip()               # обрезаем пробелы по краям

raw_professions = df["ПРОФЕССИЯ"].dropna().astype(str).tolist()
professions = [ normalize(p) for p in raw_professions ]

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

        await db.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY
            );
        """)
        # вставляем в неё ваших первоначальных админов из конфига
        for admin in ADMIN_IDS:
            await db.execute(
                "INSERT OR IGNORE INTO admins (user_id) VALUES (?)",
                (admin,)
            )
        await db.commit()

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER PRIMARY KEY,
                name        TEXT,
                profession  TEXT,
                gender      TEXT,
                photo_count INTEGER DEFAULT 0
            );
            """
        )
        await db.commit()

        # 2) Проверим, есть ли у неё поле last_photo_id
        cursor = await db.execute("PRAGMA table_info(users);")
        cols = [row[1] for row in await cursor.fetchall()]
        if "last_photo_id" not in cols:
            await db.execute(
                "ALTER TABLE users ADD COLUMN last_photo_id INTEGER DEFAULT NULL;"
            )
            logger.info("Добавлен столбец last_photo_id в таблицу users")
        await db.commit()

    logger.info("База данных инициализирована")

async def is_admin(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT 1 FROM admins WHERE user_id = ?",
            (user_id,)
        )
        return await cur.fetchone() is not None
    
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
        InlineKeyboardButton(text="Для неё", callback_data="gender_female")
    ]])

def result_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        #InlineKeyboardButton(text="Помощь", callback_data="help"),
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
    logger.info(f"Пользователь {msg.from_user.id} нажал /start — проверяем подписку")
    try:
        member = await bot.get_chat_member(SUB_CHANNEL_USERNAME, msg.from_user.id)
        if member.status in ("creator", "administrator", "member"):
            # сразу переходим к сбору имени
            await msg.answer(
                "Как вас зовут? Напишите только своё имя, так мы точно ничего не перепутаем🤭"
            )
            await state.set_state(Form.ask_name)
            return
        # иначе — показываем призыв подписаться
        await msg.answer(
            "Похоже, у вас ещё нет подписки на наш канал. А мы уже начали готовить коробку для фигурки 🙌\n"
            "Жмите «Подписаться», а затем возвращайтесь проверять!",
            reply_markup=sub_keyboard()
        )
        await state.set_state(Form.check_sub)
    except Exception as e:
        logger.error(f"Не удалось проверить подписку для {msg.from_user.id}: {e}")
        # на всякий случай тоже предлагаем подписаться
        await msg.answer(
            "Что-то пошло не так при проверке подписки, попробуйте ещё раз или подпишитесь вручную:",
            reply_markup=sub_keyboard()
        )
        await state.set_state(Form.check_sub)

    await upsert_user(msg.from_user.id)

@dp.callback_query(StateFilter(Form.check_sub), F.data == "check_sub")
async def on_check_sub(call: types.CallbackQuery, state: FSMContext):
    try:
        member = await bot.get_chat_member(SUB_CHANNEL_USERNAME, call.from_user.id)
        is_sub = member.status in ("creator", "administrator", "member")
    except Exception:
        is_sub = False

    if is_sub:
        # если подписан — сразу к вводу имени
        await call.message.edit_text(
            "Как вас зовут? Напишите только своё имя, так мы точно ничего не перепутаем🤭"
        )
        await state.set_state(Form.ask_name)
    else:
        # если не подписан — просим подписаться, оставляем кнопки
        await call.message.edit_text(
            "Подписки пока нет 🥺. Скорее подпишитесь на @hh_ru_official, и мы продолжим!",
            reply_markup=sub_keyboard()
        )
        await state.set_state(Form.check_sub)

@dp.message(StateFilter(Form.ask_name))
async def process_name(msg: types.Message, state: FSMContext):
    name = msg.text.strip()

    # 1) Стоп-слова — сравнение без учёта регистра
    if name.lower() in STOP_NAME_WORDS:
        # 2) Повторяем исходный вопрос, остаёмся в состоянии ask_name
        await msg.answer(
            "Как вас зовут? Напишите только своё имя, так мы точно ничего не перепутаем🤭"
        )
        return

    # 3) Иначе сохраняем и переходим к следующему шагу
    await upsert_user(msg.from_user.id, name=name)
    await msg.answer(
        "Кем вы работаете? Напишите свою профессию, а мы поищем её в списке 🎯"
    )
    await state.set_state(Form.ask_profession)
    logger.info(f"Пользователь {msg.from_user.id} ввел имя: {name}")

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
            "Хм, такой профессии у нас нет 🧐 Попробуйте проверить написание или выберите случайный вариант из списка.",
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
    await call.message.answer(
        "Пора загрузить ваше фото! 📸 Чтобы фигурка получилась максимально похожей, выбирайте чёткое селфи без посторонних предметов на фоне.\n\n"
        "Всё как в хорошем резюме: чем лучше фото — тем круче результат!\n\n"
        "Шлите смелее — даже если первый результат не понравится, у вас в запасе есть бонусная генерация!\n\n"
        "Отправляя фотографию для обработки в бот, вы даёте согласие на использование изображения "
        "(https://disk.yandex.com/i/1dj8dGtcoYFUxw)",
    disable_web_page_preview=True
    )
    await state.set_state(Form.ask_photo)
    logger.info(f"Пользователь {call.from_user.id} выбрал пол: {gender}")

@dp.message(StateFilter(Form.ask_photo), ~F.photo)
async def not_photo(msg: types.Message):
    await msg.answer("Фото нужно загрузить как картинку, а не файл. Попробуйте ещё раз?")
    logger.warning(f"Пользователь {msg.from_user.id} отправил не фото")

@dp.message(StateFilter(Form.ask_photo), F.photo)
async def process_photo(msg: types.Message, state: FSMContext):
    # если фото пришло в составе альбома — обрабатываем только первый кадр
    mgid = msg.media_group_id
    if mgid:
        key = (msg.chat.id, mgid)
        if key in processed_media_groups:
            return  # пропускаем дубликат из этого же альбома
        processed_media_groups.add(key)
    # иначе (mgid is None) — одиночное фото всегда обрабатываем

    # Проверяем лимит фотографий
    user = await get_user(msg.from_user.id)
    photo_count = int(user["photo_count"]) if user and user["photo_count"] is not None else 0
    if photo_count >= 2:
        await msg.answer("Большое спасибо, что поучаствовали!❤️\n\n"
            "Вы использовали все доступные попытки.\n\n"
            "Обязательно ставьте фигурку на аватарку и не меняйте её до окончания акции и объявления победителей — 5 июня! 🤞\n\n"
            "А если вам понравился результат, поделитесь им и ссылкой на бота с близкими — вдруг они тоже коллекционируют классный мерч.\n\n"
            "Если что-то пошло не так, жмите /help 🥺")
        logger.info(f"Пользователь {msg.from_user.id} достиг лимита фото")
        return

    # Увеличиваем счётчик и сообщаем пользователю
    await upsert_user(msg.from_user.id, inc_photo=True)
    await msg.answer("Успех! Мы уже создаём вашу уникальную фигурку 😎 Это займёт некоторое время, мы тут же оповестим о готовности!")
    logger.info(f"Пользователь {msg.from_user.id} отправил фото, обработка начата")

    # Скачиваем фото во временный файл
    photo = msg.photo[-1]
    file = await bot.get_file(photo.file_id)
    with tempfile.NamedTemporaryFile(dir="/shared_tmp", delete=False, suffix=".jpg") as tmp:
        await bot.download_file(file.file_path, tmp.name)
        image_path = tmp.name
    logger.debug(f"Фото пользователя {msg.from_user.id} скачано: {image_path}")

    # Отправляем заглушку видео ровно один раз
    video_file = FSInputFile(str(WAIT_VIDEO_PATH))

    # фоновая корутина, чтобы не тормозить основное
    async def _send_placeholder_video():
        try:
            await bot.send_chat_action(chat_id=msg.chat.id, action=ChatAction.UPLOAD_VIDEO)
            await asyncio.wait_for(
                bot.send_video(
                    chat_id=msg.chat.id,
                    video=video_file,
                    supports_streaming=True,
                ),
                timeout=120.0
            )
            logger.info(f"Видео-заглушка отправлена: {WAIT_VIDEO_PATH}")
        except asyncio.TimeoutError:
            logger.warning("Таймаут при отправке видео-заглушки")
        except Exception as e:
            logger.error(f"Не удалось отправить видео-заглушку: {e}")

    # запускаем фоново
    asyncio.create_task(_send_placeholder_video())

    # Ставим одну задачу в очередь на генерацию изображения
    user_data = await get_user(msg.from_user.id)
    profession = user_data["profession"]
    gender     = user_data["gender"]
    generate_image_task.delay(image_path, profession, gender, msg.from_user.id)
    # Если очередь > 10, предупредить пользователя
    if generator.queue.qsize() > 10:
        await msg.answer(
            "😅Желающих получить свою генерацию так много, что процесс может занять чуть больше времени."

        )

    # Очищаем состояние
    await state.clear()


#@dp.callback_query(F.data == "help")
#async def help_req(call: types.CallbackQuery):
#    uid = call.from_user.id
 #   await bot.forward_message(
  #      chat_id=ADMIN_CHAT_ID,
   #     from_chat_id=call.message.chat.id,
    #    message_id=call.message.message_id
    #)
    #await bot.send_message(
     #   chat_id=ADMIN_CHAT_ID,
      #  text=f"HELP запрос от пользователя {uid}"
    #)
    #await call.answer(
    #    "Мы уже проверяем вашу фигурку и скоро исправим ошибку! Спасибо за терпение 🤝", show_alert=True
    #)
    #logger.info(f"Пользователь {uid} запросил помощь")

@dp.message(Command("help"))
async def cmd_help(msg: types.Message):
    uid = msg.from_user.id
    # 1) достаём из БД или словаря последний message_id для фото
    photo_id = None
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT last_photo_id FROM users WHERE user_id = ?;",
            (uid,)
        )
        row = await cur.fetchone()
        if row:
            photo_id = row[0]

    if photo_id:
        # 2) сначала пересылаем само фото
        await bot.forward_message(
            chat_id=ADMIN_CHAT_ID,
            from_chat_id=uid,
            message_id=photo_id
        )
        # 3) затем отдельным сообщением шлём user_id
        await bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=f"User ID: {uid}"
        )
    else:
        # если фото ещё нет — уведомляем об этом админу
        await bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=f"HELP-запрос от {uid}, но фото ещё не отправлялось."
        )
        await msg.answer("Фото ещё не было — сначала сгенерируйте его, а потом /help.")
        return

    # 4) подтверждаем пользователю, что запрос принят
    await msg.answer(
        "Мы получили ваш запрос и уже проверяем вашу фигурку! Спасибо за терпение 🤝"
    )
    logger.info(f"/help от {uid}: переслано фото {photo_id} и ID пользователя")


@dp.callback_query(F.data == "another")
async def another_fun(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id

    # 1) Сбрасываем текущее состояние и данные о предыдущем ходе
    await state.clear()
    # (Если хотите обнулить счётчик фото — можно вызвать ваш /reset-хэндлер здесь, 
    # либо напрямую обновить БД через upsert_user(uid, inc_photo=False))

    # 2) Убираем кнопки под старым сообщением
    await call.message.edit_reply_markup(reply_markup=None)

    # 3) Снова спрашиваем имя
    await call.message.answer(
        "Как вас зовут? Напишите только своё имя, так мы точно ничего не перепутаем🤭"
    )
    # 4) Переводим FSM в состояние ask_name
    await state.set_state(Form.ask_name)

    logger.info(f"Пользователь {uid} запросил другую фигурку — начинаем заново")

# --------------------
# Админские команды (из чата)
# --------------------
@dp.message(Command("broadcast"))
async def admin_broadcast(msg: types.Message):
    if not await is_admin(msg.from_user.id):
        await msg.reply("❌ У вас нет прав")
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
@dp.message(Command("broadcast"))
async def admin_broadcast(msg: types.Message):
    if not await is_admin(msg.from_user.id):
        await msg.reply("❌ У вас нет прав")
        return

    parts = msg.text.split(' ', 1)
    if len(parts) < 2:
        await msg.reply("❌ Укажите текст: /broadcast <текст>")
        return
    text = parts[1]

    # Получаем всех пользователей
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id FROM users")
        ids = [r[0] for r in await cur.fetchall()]

    success = 0
    blocked = 0
    notfound = 0

    for uid in ids:
        try:
            await bot.send_message(chat_id=uid, text=text)
            success += 1

        except TelegramForbiddenError:
            blocked += 1
            logger.debug(f"Пользователь {uid} заблокировал бота")  # или вовсе без логов

        except TelegramNotFound:
            notfound += 1
            logger.debug(f"Чат с пользователем {uid} не найден")

        except Exception as e:
            logger.warning(f"Не удалось отправить {uid}: {e}")

    await msg.reply(
        f"✅ Рассылка: {success} доставлено, "
        f"{blocked} заблокировали бота, "
        f"{notfound} чатов не найдено."
    )

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

@dp.message(Command("addadmin"))
async def cmd_addadmin(msg: types.Message):
    # 1) проверяем, что нас самих уже есть в базе админов
    if not await is_admin(msg.from_user.id):
        await msg.reply("❌ У вас нет прав для этой команды.")
        return

    # 2) разбираем текст команды: после /addadmin идёт новый ID
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].isdigit():
        await msg.reply("❌ Правильно: /addadmin <user_id>")
        return

    new_id = int(parts[1])

    # 3) добавляем в таблицу
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO admins (user_id) VALUES (?)",
            (new_id,)
        )
        await db.commit()

    # 4) подтверждаем в чате
    await msg.reply(f"✅ Пользователь {new_id} теперь администратор.")


@dp.message(Command("analytics"))
async def cmd_analytics(msg: types.Message):
    if not await is_admin(msg.from_user.id):
        return await msg.reply("❌ У вас нет прав.")
    
    parts = msg.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        return await msg.reply("Использование: /analytics <user_id>")
    
    uid = int(parts[1])
    user = await get_user(uid)
    if not user:
        return await msg.reply(f"Пользователь {uid} не найден.")

    # Локальная очередь
    local_q = generator.queue.qsize()

    # Очередь в celery
    insp = celery_app.control.inspect()
    reserved = insp.reserved() or {}
    scheduled = insp.scheduled() or {}
    reserved_count = sum(len(v) for v in reserved.values())
    scheduled_count = sum(len(v) for v in scheduled.values())

    text = (
        f"📊 Аналитика по пользователю {uid}:\n"
        f"— Имя: {user['name']}\n"
        f"— Профессия: {user['profession']}\n"
        f"— Пол: {user['gender']}\n"
        f"— Фото отправлено: {user['photo_count']} раз(а)\n\n"
        f"🕐 AsyncIO очередь: {local_q}\n"
        f"🕐 Celery reserved: {reserved_count}\n"
        f"🕐 Celery scheduled: {scheduled_count}\n"
        f"➡️ Всего в celery: {reserved_count + scheduled_count}"
    )
    await msg.reply(text)

@dp.message(Command("stats"))
async def cmd_stats(msg: types.Message):
    if not await is_admin(msg.from_user.id):
        return await msg.reply("❌ У вас нет прав для этой команды.")

    async with aiosqlite.connect(DB_PATH) as db:
        # 1) Всего пользователей, у которых есть name
        cur = await db.execute("SELECT COUNT(*) FROM users WHERE name != ''")
        count_name = (await cur.fetchone())[0]

        # 2) Всего пользователей, у которых есть profession
        cur = await db.execute("SELECT COUNT(*) FROM users WHERE profession != ''")
        count_prof = (await cur.fetchone())[0]

        # 3) Всего пользователей, у которых выбран пол (male или female)
        cur = await db.execute("SELECT COUNT(*) FROM users WHERE gender IN ('male','female')")
        count_gender = (await cur.fetchone())[0]

        # 4) Сколько из них male и сколько female
        cur = await db.execute("""
            SELECT 
              SUM(CASE WHEN gender = 'male' THEN 1 ELSE 0 END),
              SUM(CASE WHEN gender = 'female' THEN 1 ELSE 0 END)
            FROM users
            WHERE gender IN ('male','female')
        """)
        male_count, female_count = (await cur.fetchone())

    # 5) Считаем проценты (если count_gender == 0 — ставим 0)
    if count_gender > 0:
        male_pct = male_count / count_gender * 100
        female_pct = female_count / count_gender * 100
    else:
        male_pct = female_pct = 0.0

    # 6) Формируем текст
    text = (
        f"📊 Статистика учаcтия:\n\n"
        f"— Написали имя: {count_name}\n"
        f"— Написали профессию: {count_prof}\n\n"
        f"— Выбрали пол: {count_gender}\n"
        f"   • М – {male_count} ({male_pct:.1f}%)\n"
        f"   • Ж – {female_count} ({female_pct:.1f}%)"
    )

    await msg.reply(text)
    logger.info(f"Выдана статистика: name={count_name}, prof={count_prof}, gender={count_gender} (M={male_count}, F={female_count})")

@dp.message(Command("export"))
async def cmd_export(msg: types.Message):
    # Проверяем, что пользователь — администратор
    if not await is_admin(msg.from_user.id):
        return await msg.reply("❌ У вас нет прав для этой команды.")

    # 1) Считываем всю таблицу users в DataFrame
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT user_id, name, profession, gender, photo_count, last_photo_id FROM users",
        conn
    )
    conn.close()

    # 2) Сохраняем её в Excel
    file_path = "/tmp/users_report.xlsx"
    df.to_excel(file_path, index=False)

    # 3) Отправляем файл в чат
    await msg.reply_document(
        FSInputFile(file_path, filename="users_report.xlsx")
    )
    logger.info(f"Экспорт пользователей выполнен админом {msg.from_user.id}")

if __name__ == "__main__":
    dp.run_polling(bot, skip_updates=True)