import os
import asyncio
import logging
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import CommandStart, Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED, EVENT_JOB_EXECUTED
from aiogram.client.default import DefaultBotProperties

import app.handlers as handlers
from app.handlers import router
from app.database.models import async_main
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Включаем HTML-разметку по умолчанию для сообщений (нужна для жирных заголовков отчётов)
bot = Bot(token=str(os.getenv("TOKEN")), default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone="Europe/Moscow")


@dp.message(Command("test_daily"))
async def test_daily_report(message: Message):
    await handlers.day_res(bot)


@dp.message(Command("test_weekly"))
async def test_weekly_report(message: Message):
    await handlers.week_res(bot)


@dp.message(Command("test_scheduler"))
async def test_scheduler(message: Message):
    if message.chat.type != 'private':
        return

    daily_run_time = datetime.now(scheduler.timezone) + timedelta(minutes=1)
    weekly_run_time = datetime.now(scheduler.timezone) + timedelta(minutes=2)

    scheduler.add_job(
        handlers.day_res,
        "date",
        id="test_daily_report",
        replace_existing=True,
        run_date=daily_run_time,
        args=[bot],
        max_instances=1,
    )
    scheduler.add_job(
        handlers.week_res,
        "date",
        id="test_weekly_report",
        replace_existing=True,
        run_date=weekly_run_time,
        args=[bot],
        kwargs={"force": True},
        max_instances=1,
    )

    await message.answer(
        "Тестовые задачи добавлены: daily через 1 минуту, weekly через 2 минуты."
    )


@dp.message(CommandStart())
async def cmd_start(message: Message):
    await bot.send_message(chat_id='629967123', text=str(message.chat.id))


# Инициализация планировщика
async def on_startup(dispatcher):
    """Настройка планировщика при запуске бота."""
    if scheduler.running:
        print("Планировщик уже запущен")
        return
    # Ежедневный отчет в 23:59
    scheduler.add_job(
        handlers.day_res,
        "cron",
        id="daily_report",
        replace_existing=True,
        hour=23,
        minute=55,
        args=[bot],
        coalesce=True,
        max_instances=1,
        misfire_grace_time=300,
    )
    # Еженедельный отчет в воскресенье в 23:59
    scheduler.add_job(
        handlers.week_res,
        "cron",
        id="weekly_report",
        replace_existing=True,
        day_of_week="sun",
        hour=23,
        minute=56,
        args=[bot],
        coalesce=True,
        max_instances=1,
        misfire_grace_time=300,
    )
    print("Зарегистрированные задачи:", [job.id for job in scheduler.get_jobs()])
    print("CHAT_ID:", os.getenv("CHAT_ID"))
    scheduler.start()
    for job in scheduler.get_jobs():
        print(f"{job.id}: next_run_time={getattr(job, 'next_run_time', None)}")
    print("Планировщик запущен")


def _scheduler_listener(event):
    if event.code == EVENT_JOB_MISSED:
        logger.warning("Job missed: %s", event.job_id)
    elif event.code == EVENT_JOB_ERROR:
        logger.exception("Job failed: %s", event.job_id, exc_info=event.exception)
    elif event.code == EVENT_JOB_EXECUTED:
        logger.info("Job executed: %s", event.job_id)


async def on_shutdown(dispatcher):
    """Остановка планировщика."""
    scheduler.shutdown()
    print("Планировщик остановлен")


async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    dp.include_router(router)
    scheduler.add_listener(_scheduler_listener, EVENT_JOB_ERROR | EVENT_JOB_MISSED)
    await async_main()
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
    print("Бот остановлен")
