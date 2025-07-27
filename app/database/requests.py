import os
import pytz
from datetime import datetime, timedelta
from sqlalchemy import select, update, delete, func
from app.database.models import async_session, Agent, DailyMessage, Client, Admin, Norm


async def is_admin(username):
    async with async_session() as session:
        username = username.strip('@')
        admin = await session.scalar(select(Admin).where(Admin.username == username))
        if admin:
            us = admin.username
            cre = admin.is_creator
            await session.commit()
            return True, us, cre
        else:
            await session.commit()
            return False, None, 0


async def delete_admin(from_user, username):
    async with async_session() as session:
        username = username.strip('@')
        admin1 = await session.scalar(select(Admin).where(Admin.username == from_user.username))
        admin2 = await session.scalar(select(Admin).where(Admin.username == username))
        if admin1.is_creator > admin2.is_creator:
            await session.execute(delete(Admin).where(Admin.username == username))
            await session.commit()
            return True
        await session.commit()
        return False


async def all_admins():
    async with async_session() as session:
        res = []
        nicknames = []
        admin = await session.scalars(select(Admin))
        for a in admin:
            nicknames.append(a.username)
            res.append(f'@{a.username}')
        return res, nicknames


async def set_client(username1):
    async with async_session() as session:
        client = await session.scalar(select(Client).where(Client.username == username1))
        if not client:
            session.add(Client(username=username1))
            await session.commit()


async def set_admin(username1):
    async with async_session() as session:
        admin = await session.scalar(select(Admin).where(Admin.username == username1))
        if not admin:
            session.add(Admin(username=username1, is_creator=0))
            await session.commit()


async def all_agents():
    async with async_session() as session:
        res = []
        nicknames = []
        agents = await session.scalars(select(Agent))
        for agent in agents:
            nicknames.append(agent.nickname)
            res.append(f'Агент: @{agent.nickname}\nТекущая норма: {agent.norm_rate}')
        return res, nicknames


async def all_clients():
    async with async_session() as session:
        res = []
        nicknames = []
        clients = await session.scalars(select(Client))
        for client in clients:
            nicknames.append(client.username)
            res.append(f'@{client.username}')
        return res, nicknames


async def all_time_messages():
    async with async_session() as session:
        messages = await session.scalars(select(DailyMessage))
        return sum([message.dialogs_count for message in messages])


async def count_day(current_date):
    async with async_session() as session:
        count = await session.scalars(select(DailyMessage).where(DailyMessage.date == current_date))
        return sum([int(i.dialogs_count) for i in count])


async def all_daily_messages(current_date):
    async with async_session() as session:
        res = []
        messages = await session.scalars(select(DailyMessage).where(DailyMessage.date == current_date))
        for message in messages:
            agent = await session.scalar(select(Agent).where(Agent.tg_id == message.tg_id))
            if message.dialogs_count % 10 == 1:
                soo = 'сообщение'
            elif 2 <= message.dialogs_count % 10 <= 4:
                soo = 'сообщения'
            else:
                soo = "сообщений"
            res.append(f'Агент @{agent.nickname} - {message.dialogs_count} {soo}')
        return res


async def get_norm():
    async with async_session() as session:
        norm = await session.scalar(select(Norm).where(Norm.id == 1))
        return norm.norm


async def set_new_norm(new_norm: int):
    async with async_session() as session:
        await session.execute(update(Norm).where(Norm.id == 1).values(norm=new_norm))
        await session.commit()


async def reset_norm(username, norm):
    async with async_session() as session:
        await session.execute(update(Agent).where(Agent.nickname == username)
                              .values(norm_rate=norm))
        await session.commit()


async def add_dialog(agent_username, client, current_date):
    async with async_session() as session:
        try:
            agent = await session.scalar(select(Agent).where(Agent.nickname == agent_username))
            daily_message = await session.scalar(select(DailyMessage)
                                                 .where(DailyMessage.tg_id == agent.tg_id,
                                                        DailyMessage.date == current_date))
            client = await session.scalar(select(Client).where(Client.username == client))
            if not agent:
                return 'not_agent'
            if not client:
                session.add(Client(username=client))
            else:
                return 'повтор'
            if not daily_message:
                session.add(DailyMessage(tg_id=agent.tg_id, date=current_date, dialogs_count=1))
            else:
                await session.execute(update(DailyMessage).where(
                    DailyMessage.tg_id == daily_message.tg_id, DailyMessage.date == daily_message.date
                ).values(dialogs_count=daily_message.dialogs_count + 1))
            await session.commit()
        except Exception as e:
            print(f"Ошибка в count_daily_messages: {e}")
            await session.rollback()


async def delete_dialog(agent_username, client, current_date):
    async with async_session() as session:
        agent = await session.scalar(select(Agent).where(Agent.nickname == agent_username))
        daily_message = await session.scalar(select(DailyMessage)
                                             .where(DailyMessage.tg_id == agent.tg_id,
                                                    DailyMessage.date == current_date))
        await session.execute(update(DailyMessage)
                              .where(DailyMessage.tg_id == agent.tg_id, DailyMessage.date == current_date)
                              .values(dialogs_count=max(daily_message.dialogs_count-1, 0))
                              )
        await session.execute(delete(Client).where(Client.username == client))
        await session.commit()


async def set_agent(from_user):
    async with async_session() as session:
        agent = await session.scalar(select(Agent).where(Agent.tg_id == from_user.id))
        if not agent:
            norm = await get_norm()
            session.add(Agent(tg_id=from_user.id, nickname=from_user.username, norm_rate=norm))
            await session.commit()


async def count_daily_messages(from_user, current_date, message):
    async with async_session() as session:
        try:
            daily_message = await session.scalar(select(DailyMessage).where(
                DailyMessage.tg_id == from_user.id, DailyMessage.date == current_date
            ))
            client = await session.scalar(select(Client).where(Client.username == message.caption[1:]))
            if not client:
                session.add(Client(username=message.caption[1:]))
            else:
                return True

            if not daily_message:
                session.add(DailyMessage(tg_id=from_user.id, date=current_date, dialogs_count=1))
            else:
                await session.execute(update(DailyMessage).where(
                    DailyMessage.tg_id == daily_message.tg_id, DailyMessage.date == daily_message.date
                ).values(dialogs_count=daily_message.dialogs_count + 1))
            await session.commit()
        except Exception as e:
            print(f"Ошибка в count_daily_messages: {e}")
            await session.rollback()


async def daily_results(current_date):
    dct = {}
    async with async_session() as session:
        try:
            daily_messages = await session.scalars(select(DailyMessage).where(DailyMessage.date == current_date))
            for message in daily_messages:
                old_norm_ = await session.scalar(select(Agent).where(Agent.tg_id == message.tg_id))
                old_norm = old_norm_.norm_rate
                norm = await get_norm()
                bonuses = 0
                if message.dialogs_count >= old_norm:
                    bonuses = int(os.getenv('BONUSES')) * int((message.dialogs_count - old_norm) / 5)
                    salary = int(os.getenv('SALARY')) + bonuses
                    norm = norm - (message.dialogs_count - old_norm) % 5
                else:
                    salary = 0
                    norm = old_norm - message.dialogs_count

                await session.execute(update(Agent).where(Agent.tg_id == message.tg_id).values(norm_rate=norm))
                await session.execute(update(DailyMessage)
                                      .where(DailyMessage.tg_id == message.tg_id, DailyMessage.date == message.date)
                                      .values(salary=salary))

                dct[message.tg_id] = [old_norm_.nickname, message.dialogs_count, bonuses, salary, old_norm, norm]
            await session.commit()
            return dct
        except Exception as e:
            print(f"Ошибка в daily_results: {e}")
            await session.rollback()
            return {}


async def get_week_date_range(current_date: datetime.date):
    days_since_monday = current_date.weekday()
    monday = current_date - timedelta(days=days_since_monday)
    sunday = monday + timedelta(days=6)
    return monday, sunday


async def weekly_results():
    msk_tz = pytz.timezone("Europe/Moscow")
    current_date = datetime.now(msk_tz).date()
    if current_date.weekday() != 6:  # Fixed: 6 is Sunday, not 2
        return ""

    monday, sunday = await get_week_date_range(current_date)
    monday_str = monday.strftime("%Y-%m-%d")
    sunday_str = sunday.strftime("%Y-%m-%d")

    async with async_session() as session:
        try:
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

            mon = monday_str.split('-')
            mon = f'{mon[2]}.{mon[1]}'
            sun = sunday_str.split('-')
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
            return "\n\n".join(report_lines)
        except Exception as e:
            print(f"Ошибка в weekly_results: {e}")
            await session.rollback()
            return f"Ошибка при формировании отчета: {e}"
