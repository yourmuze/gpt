import aiosqlite
import logging

import os
DB_PATH = os.getenv("USERS_DB", "data/users.db")

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER UNIQUE,
                is_subscribed BOOLEAN DEFAULT 0,
                category TEXT,
                detail TEXT,
                name TEXT,
                city TEXT,
                address TEXT,
                review TEXT,
                genre TEXT,
                gen_limit INTEGER DEFAULT 1,
                current_gen_count INTEGER DEFAULT 0,
                generator TEXT DEFAULT 'suno'
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS song_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER,
                category TEXT,
                detail TEXT,
                name TEXT,
                city TEXT,
                address TEXT,
                review TEXT,
                genre TEXT,
                prompt TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Новая таблица settings
        await db.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        try:
            await db.execute("ALTER TABLE users ADD COLUMN generator TEXT DEFAULT 'suno'")
            await db.commit()
            logging.info("Столбец generator добавлен в users")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" in str(e):
                logging.info("Столбец generator уже существует")
            else:
                logging.error(f"Ошибка при добавлении колонки generator: {e}")

        try:
            await db.execute('ALTER TABLE users ADD COLUMN is_finished INTEGER DEFAULT 0')
            await db.commit()
            logging.info("Столбец is_finished добавлен в users")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" in str(e):
                logging.info("Столбец is_finished уже существует")
            else:
                logging.error(f"Ошибка при добавлении колонки is_finished: {e}")

        # Устанавливаем начальное значение глобального лимита (например, 1)
        await db.execute('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', ('default_gen_limit', '1'))
        await db.commit()
        logging.info("[DB] База данных инициализирована.")


async def init_settings_table():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
          CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
          )
        """)
        await db.commit()

async def set_global_generator(gen: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
          INSERT INTO settings(key, value)
          VALUES ('generator', ?)
          ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """, (gen,))
        await db.commit()

async def get_global_generator() -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT value FROM settings WHERE key='generator'"
        )
        row = await cur.fetchone()
    return row[0] if row else "suno"

async def add_user(tg_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        # Получаем глобальный лимит из settings
        async with db.execute("SELECT value FROM settings WHERE key = 'default_gen_limit'") as cursor:
            row = await cursor.fetchone()
            default_limit = int(row[0]) if row else 1  # Если нет значения, используем 1
        # Добавляем пользователя с глобальным лимитом
        await db.execute(
            'INSERT OR IGNORE INTO users (tg_id, gen_limit, current_gen_count) VALUES (?, ?, 0)',
            (tg_id, default_limit)
        )
        await db.commit()
        logging.info(f"[DB] Добавлен пользователь {tg_id} с gen_limit={default_limit}")

async def set_gen_limit(limit: int):
    async with aiosqlite.connect(DB_PATH) as db:
        # Обновляем глобальный лимит в settings
        await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('default_gen_limit', ?)", (str(limit),))
        # Применяем новый лимит ко всем пользователям
        await db.execute("UPDATE users SET gen_limit = ?", (limit,))
        await db.commit()
        logging.info(f"[DB] Установлен глобальный лимит генераций: {limit}")

async def get_user(tg_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT * FROM users WHERE tg_id=?', (tg_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                columns = [col[0] for col in cursor.description]
                return dict(zip(columns, row))
            return None

async def update_user_fields(tg_id: int, **fields):
    if not fields:
        return
    keys = ', '.join(f"{k}=?" for k in fields)
    values = list(fields.values())
    values.append(tg_id)
    query = f"UPDATE users SET {keys} WHERE tg_id=?"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(query, values)
        await db.commit()
        logging.info(f"[DB] Обновлены поля {fields.keys()} для пользователя {tg_id}")

async def clear_user(tg_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            UPDATE users
            SET is_subscribed=0,
                category=NULL,
                detail=NULL,
                name=NULL,
                city=NULL,
                address=NULL,
                review=NULL,
                genre=NULL,
                current_gen_count=0,
                is_finished=0
            WHERE tg_id=?
        ''', (tg_id,))
        await db.commit()
        logging.info(f"[DB] Данные пользователя {tg_id} сброшены.")

async def add_song_history(tg_id: int, data: dict, prompt: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO song_history (tg_id, category, detail, name, city, address, review, genre, prompt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            tg_id,
            data.get("category"),
            data.get("detail"),
            data.get("name"),
            data.get("city"),
            data.get("address"),
            data.get("review"),
            data.get("genre"),
            prompt
        ))
        await db.commit()

async def get_user_history(tg_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            '''SELECT id, category, detail, name, city, address, review, genre, prompt, created_at
               FROM song_history WHERE tg_id=?
               ORDER BY created_at ASC''', (tg_id,)
        ) as cursor:
            return await cursor.fetchall()

async def delete_user_history(tg_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        # Удалить все прохождения из song_history
        await db.execute('DELETE FROM song_history WHERE tg_id=?', (tg_id,))
        # Очистить анкету пользователя (users)
        await db.execute('''
            UPDATE users
            SET is_subscribed=0,
                category=NULL,
                detail=NULL,
                name=NULL,
                city=NULL,
                address=NULL,
                review=NULL,
                genre=NULL,
                current_gen_count=0
            WHERE tg_id=?
        ''', (tg_id,))
        await db.commit()

async def get_all_users():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT tg_id FROM users') as cursor:
            return await cursor.fetchall()
        
async def set_category(tg_id: int, category: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE users SET category=? WHERE tg_id=?', (category, tg_id))
        await db.commit()
        logging.info(f"[DB] Пользователь {tg_id} выбрал категорию: {category}")