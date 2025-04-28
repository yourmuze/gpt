from aiogram import executor
from bot_handlers import dp

if __name__ == '__main__':
    # Запуск бота aiogram
    executor.start_polling(dp, skip_updates=True)
    # Celery worker и beat запускаются через Docker-Compose