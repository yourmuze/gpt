#!/usr/bin/env python3
import time
import redis
import logging
import argparse
from logging.handlers import RotatingFileHandler
from config import REDIS_URL
from celery_app import celery_app
from tasks import generate_image_task

# --- Константы по умолчанию (можно переопределить через --count, --profession и т.д.) ---
DEFAULT_TASK_COUNT = 12
TEST_IMAGE  = "/app/placeholder.jpg"
PROFESSION  = "врач"
GENDER      = "male"

# --- Настройка логирования ---
LOG_FILE = "queue_test.log"
logger = logging.getLogger("queue_test")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(
    LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=2, encoding="utf-8"
)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def enqueue_tasks(n: int, profession: str, gender: str, image_path: str):
    """
    Ставит в очередь n задач generate_image_task.
    """
    logger.info(f"Начало постановки в очередь {n} задач "
                f"(profession={profession}, gender={gender})")
    for i in range(n):
        user_id = 1_000_000 + i
        try:
            generate_image_task.delay(image_path, profession, gender, user_id)
            logger.debug(f"Задача {i+1}/{n} поставлена: user_id={user_id}")
        except Exception as e:
            logger.error(f"Ошибка при постановке задачи {i+1}/{n} "
                         f"(user_id={user_id}): {e}")
    logger.info("Все задачи поставлены в очередь.")


def monitor_queue():
    """
    Каждую секунду проверяет длину очереди Redis until она обнулится.
    """
    logger.info("Начало мониторинга очереди Celery")
    r = redis.Redis.from_url(REDIS_URL)
    while True:
        try:
            length = r.llen("celery")
            logger.info(f"Осталось задач в очереди: {length}")
            if length == 0:
                logger.info("Очередь опустела — все задачи отработаны.")
                break
        except Exception as e:
            logger.error(f"Ошибка при проверке длины очереди: {e}")
        time.sleep(1)


def inspect_workers():
    """
    Выводит активные и зарезервированные задачи у всех воркеров.
    """
    logger.info("Инспекция состояния воркеров Celery")
    insp = celery_app.control.inspect()
    try:
        active   = insp.active() or {}
        reserved = insp.reserved() or {}
        logger.info(f"Active tasks: {active}")
        logger.info(f"Reserved tasks: {reserved}")
    except Exception as e:
        logger.error(f"Ошибка инспекции воркеров: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Тест производительности Celery-очереди"
    )
    parser.add_argument(
        "--count", "-c", type=int, default=DEFAULT_TASK_COUNT,
        help=f"Сколько задач поставить в очередь (default={DEFAULT_TASK_COUNT})"
    )
    parser.add_argument(
        "--profession", "-p", type=str, default=PROFESSION,
        help=f"Профессия для тестовых задач (default='{PROFESSION}')"
    )
    parser.add_argument(
        "--gender", "-g", type=str, default=GENDER,
        choices=["male", "female"],
        help=f"Пол для тестовых задач (default='{GENDER}')"
    )
    parser.add_argument(
        "--image", "-i", type=str, default=TEST_IMAGE,
        help=f"Путь до тестового изображения (default='{TEST_IMAGE}')"
    )
    args = parser.parse_args()

    try:
        enqueue_tasks(args.count, args.profession, args.gender, args.image)
        monitor_queue()
        inspect_workers()
    except Exception as e:
        logger.critical(f"Непредвиденная ошибка в queue_test: {e}", exc_info=True)
    else:
        logger.info("Тест очереди завершён успешно.")
