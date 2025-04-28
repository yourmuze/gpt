from celery import Celery
from config import REDIS_URL

celery_app = Celery(
    'image_tasks',
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=['tasks']       # <--- добавляем ваш модуль с тасками
)

celery_app.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
)
