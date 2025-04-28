from celery import Celery
from celery.exceptions import MaxRetriesExceededError
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from config import REDIS_URL, SELENIUM_HUB_URL, TIMEOUT, CIRCUIT_BREAKER_KEY, CIRCUIT_BREAKER_TIMEOUT, CIRCUIT_BREAKER_MAX_ERRORS, NUM_SESSIONS, ADMIN_CHAT_ID
from sessions import SessionManager
from prompt_builder import build_prompt
import redis
import requests
from io import BytesIO

# Инициализация Celery
app = Celery('tasks', broker=REDIS_URL, backend=REDIS_URL)

# Подключение к Redis
redis_client = redis.Redis.from_url(REDIS_URL)

# Менеджер сессий
session_manager = SessionManager(num_sessions=NUM_SESSIONS)

@app.task(bind=True, max_retries=3)
def generate_image(self, image_bytes: bytes, user_text: str, style: str, palette: str, chat_id: int, reply_msg_id: int):
    """Задача для генерации изображения через DALL·E."""
    # Проверка состояния Circuit-Breaker
    if redis_client.get(CIRCUIT_BREAKER_KEY):
        raise Exception("Circuit breaker открыт, попробуйте позже")

    # Получение доступной сессии
    session = session_manager.get_available_session()
    if not session:
        self.retry(countdown=10)  # Повтор через 10 секунд, если сессий нет

    try:
        driver = session.driver

        # Загрузка страницы chat.openai.com
        driver.get('https://chat.openai.com')

        # Ожидание кнопки "Attach a file"
        attach_button = WebDriverWait(driver, TIMEOUT).until(
            EC.element_to_be_clickable((By.XPATH, "//button[@aria-label='Attach a file']"))
        )
        attach_button.click()

        # Загрузка изображения (реализация через временный файл или Selenium API)
        # Примечание: для image_bytes требуется дополнительная логика (временно пропущена)

        # Ввод промта
        prompt = build_prompt(user_text, style, palette)
        textarea = driver.find_element(By.XPATH, "//textarea[@aria-label='Send a message']")
        textarea.send_keys(prompt)

        # Нажатие кнопки отправки
        send_button = driver.find_element(By.XPATH, "//button[@aria-label='Send message']")
        send_button.click()

        # Ожидание результата (заглушка, требуется реальная логика)
        image_url = "https://example.com/generated_image.jpg"  # Заменить на реальный URL

        # Скачивание изображения через requests
        cookies = driver.get_cookies()
        session_cookies = {cookie['name']: cookie['value'] for cookie in cookies}
        headers = {'User-Agent': driver.execute_script("return navigator.userAgent;")}
        response = requests.get(image_url, cookies=session_cookies, headers=headers)
        result_bytes = response.content

        # Отправка изображения пользователю
        from bot_handlers import bot
        bot.send_photo(chat_id=chat_id, photo=BytesIO(result_bytes), reply_to_message_id=reply_msg_id)

        # Сброс счётчика ошибок
        redis_client.set(f"error_count_{session.id}", 0)

    except Exception as e:
        # Увеличение счётчика ошибок
        error_count = int(redis_client.get(f"error_count_{session.id}") or 0) + 1
        redis_client.set(f"error_count_{session.id}", error_count)

        # Открытие Circuit-Breaker при превышении ошибок
        if error_count >= CIRCUIT_BREAKER_MAX_ERRORS:
            redis_client.set(CIRCUIT_BREAKER_KEY, 'open', ex=CIRCUIT_BREAKER_TIMEOUT)
            from bot_handlers import bot
            bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"Circuit breaker открыт из-за ошибок в сессии {session.id}")

        # Повтор при ошибке
        try:
            self.retry(exc=e, countdown=10)
        except MaxRetriesExceededError:
            from bot_handlers import bot
            bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"Превышено максимальное число повторов для задачи {self.request.id}")

    finally:
        session_manager.release_session(session)