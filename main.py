import os
import asyncio

from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import CommandStart
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import app.handlers as handlers
from app.handlers import router
from app.database.models import async_main
from dotenv import load_dotenv

load_dotenv()

bot = Bot(token=os.getenv("TOKEN"))
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone="Europe/Moscow")


@dp.message(CommandStart())
async def cmd_start(message: Message):
    await bot.send_message(chat_id='629967123', text=str(message.chat.id))


# Инициализация планировщика
async def on_startup(dispatcher):
    """Настройка планировщика при запуске бота."""
    # Ежедневный отчет в 23:59
    scheduler.add_job(handlers.day_res, "cron", hour=23, minute=59, args=[bot])
    # Еженедельный отчет в воскресенье в 23:59
    scheduler.add_job(handlers.week_res, "cron", day_of_week="sun", hour=23, minute=59, second=30, args=[bot])
    scheduler.start()
    print("Планировщик запущен")


async def on_shutdown(dispatcher):
    """Остановка планировщика."""
    scheduler.shutdown()
    print("Планировщик остановлен")


async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    dp.include_router(router)
    await async_main()
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
