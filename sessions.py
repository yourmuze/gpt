import threading
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from config import SELENIUM_HUB_URL, PROFILES_DIR

class Session:
    """Класс для управления отдельной сессией Selenium."""
    def __init__(self, id):
        self.id = id + 1  # ID начинается с 1
        self.profile_path = f"{PROFILES_DIR}/profile_{self.id}"  # Путь к профилю сессии
        self.status = 'available'  # Статус: available или busy
        self.lock = threading.Lock()  # Блокировка для потокобезопасности
        self.driver = None  # Экземпляр WebDriver

    def start(self):
        """Запуск сессии с настройкой профиля."""
        with self.lock:
            self.status = 'busy'
            options = Options()
            options.add_argument(f"user-data-dir={self.profile_path}")
            self.driver = webdriver.Remote(command_executor=SELENIUM_HUB_URL, options=options)

    def stop(self):
        """Остановка сессии и освобождение ресурсов."""
        with self.lock:
            if self.driver:
                self.driver.quit()
            self.status = 'available'

class SessionManager:
    """Класс для управления пулом сессий."""
    def __init__(self, num_sessions):
        self.sessions = [Session(i) for i in range(num_sessions)]  # Создание пула сессий с id от 1 до num_sessions
        self.lock = threading.Lock()

    def get_available_session(self):
        """Получение доступной сессии."""
        with self.lock:
            for session in self.sessions:
                if session.status == 'available':
                    session.start()  # Запускаем сессию
                    return session
            return None  # Нет доступных сессий

    def release_session(self, session):
        """Освобождение сессии."""
        with self.lock:
            session.stop()  # Останавливаем сессию