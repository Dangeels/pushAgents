import os
import pytz
from datetime import datetime, timedelta
from app.database.models import async_session
from app.database.models import Agent, DailyMessage, Client
from sqlalchemy import select, update, func


async def set_agent(from_user):  # работает не трогать
    async with async_session() as session:
        agent = await session.scalar(select(Agent).where(Agent.tg_id == from_user.id))

        if not agent:
            session.add(Agent(tg_id=from_user.id, nickname=from_user.username, norm_rate=os.getenv('NORM')))
            await session.commit()


async def count_daily_messages(from_user, current_date, message):  # работает не трогать

    async with async_session() as session:
        daily_message = await session.scalar(select(DailyMessage).where(
            DailyMessage.tg_id == from_user.id, DailyMessage.date == current_date
        ))
        client = await session.scalar(select(Client).where(Client.username == message.caption[1:]))

        if not client:
            session.add(Client(username=message.caption[1:]))
        else:
            return
        if not daily_message:
            session.add(DailyMessage(
                tg_id=from_user.id,
                date=current_date,
                dialogs_count=1
            ))
        else:
            await session.execute(update(DailyMessage).where(
                DailyMessage.tg_id == daily_message.tg_id, DailyMessage.date == daily_message.date)
                                  .values(dialogs_count=daily_message.dialogs_count+1))
        await session.commit()


async def daily_results(current_date):  # работает не трогать
    dct = {}
    async with async_session() as session:
        daily_messages = await session.scalars(select(DailyMessage).where(DailyMessage.date == current_date))

        for message in daily_messages:
            old_norm_ = await session.scalar(select(Agent).where(Agent.tg_id == message.tg_id))
            old_norm = old_norm_.norm_rate
            norm = int(os.getenv('NORM'))
            bonuses = 0
            if message.dialogs_count >= old_norm:
                bonuses = int(os.getenv('BONUSES'))*int((message.dialogs_count-old_norm)/5)
                salary = int(os.getenv('SALARY'))+bonuses
                norm = norm-(message.dialogs_count-old_norm) % 5
            else:
                salary = 0
                norm = old_norm-message.dialogs_count

            await session.execute(update(Agent).where(Agent.tg_id == message.tg_id).values(norm_rate=norm))
            await session.execute(update(DailyMessage)
                                  .where(DailyMessage.tg_id == message.tg_id, DailyMessage.date == message.date)
                                  .values(salary=salary))

            dct[message.tg_id] = [old_norm_.nickname, message.dialogs_count, bonuses, message.salary, old_norm, norm]

        await session.commit()
        return dct


async def get_week_date_range(current_date: datetime.date):
    """Возвращает диапазон дат с понедельника по воскресенье для недели, содержащей current_date."""
    # Находим понедельник (вычитаем дни до понедельника)
    days_since_monday = current_date.weekday()  # 0 = понедельник, 6 = воскресенье
    monday = current_date - timedelta(days=days_since_monday)
    sunday = monday + timedelta(days=6)
    return monday, sunday


async def weekly_results():  # вроде работает
    """Генерирует и отправляет еженедельный отчет за период с понедельника по воскресенье."""
    msk_tz = pytz.timezone("Europe/Moscow")
    current_date = datetime.now(msk_tz).date()

    # Проверяем, что сегодня воскресенье
    if current_date.weekday() != 6:  # 6 = воскресенье
        return

    # Получаем диапазон дат
    monday, sunday = await get_week_date_range(current_date)
    monday_str = monday.strftime("%Y-%m-%d")
    sunday_str = sunday.strftime("%Y-%m-%d")

    async with async_session() as session:
        result = await session.execute(
            select(
                DailyMessage.tg_id,
                Agent.nickname,
                func.sum(DailyMessage.dialogs_count).label("total_dialogs"),
                func.sum(DailyMessage.salary).label("total_salary")
            )
            .join(Agent, DailyMessage.tg_id == Agent.tg_id)
            .where(DailyMessage.date >= monday_str, DailyMessage.date <= sunday_str)
            .group_by(DailyMessage.tg_id, Agent.nickname)
        )
        rows = result.all()

        mon = str(monday_str).split('-')
        mon = f'{mon[2]}.{mon[1]}'
        sun = str(sunday_str).split('-')
        sun = f'{sun[2]}.{sun[1]}'

        report_lines = [f"Итоговый отчет за неделю ({mon} - {sun}):"]
        if not rows:
            report_lines.append("Нет данных за указанный период.")
        else:
            for row in rows:
                tg_id, nickname, total_dialogs, total_salary = row
                report_lines.append(
                    f"Агент: @{nickname}\n"
                    f"Диалогов за неделю: {total_dialogs}\n"
                    f"Зарплата за неделю: {total_salary} рублей"
                )

        # Отправляем отчет в чат
        report = "\n\n".join(report_lines)

        await session.commit()
    return report
