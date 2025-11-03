import pytz
from datetime import datetime, timedelta
from sqlalchemy import select, update, delete, func
from app.database.models import async_session, Agent, DailyMessage, Client, Admin
from app.database.models import AgentGroup, AgentGroupMember
from app.database.models import Settings, AgentAdjustment


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
        if admin1 and admin2 and admin1.is_creator > admin2.is_creator:
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
            res.append(f'Агент: @{agent.nickname}')
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


# ===== Настройки выплат и норм =====
async def _get_settings(session):
    st = await session.scalar(select(Settings))
    if not st:
        st = Settings()
        session.add(st)
        await session.commit()
        await session.refresh(st)
    return st


async def get_settings():
    async with async_session() as session:
        return await _get_settings(session)


async def set_rate_per_dialog(new_rate: int):
    async with async_session() as session:
        st = await _get_settings(session)
        st.rate_per_dialog = int(new_rate)
        await session.commit()


async def set_norms_enabled(enabled: bool):
    async with async_session() as session:
        st = await _get_settings(session)
        st.norms_enabled = 1 if enabled else 0
        await session.commit()


async def set_global_norm_rate(norm_rate: int):
    async with async_session() as session:
        st = await _get_settings(session)
        st.global_norm_rate = int(norm_rate)
        await session.commit()


async def set_agent_norm(nickname: str, norm_rate: int):
    async with async_session() as session:
        ag = await session.scalar(select(Agent).where(Agent.nickname == nickname))
        if ag:
            ag.norm_rate = int(norm_rate)
            await session.commit()
            return True
        return False


# ===== Корректировки диалогов =====
async def add_adjustment(nickname: str, delta: int, date_str: str, reason: str = ""):
    async with async_session() as session:
        ag = await session.scalar(select(Agent).where(Agent.nickname == nickname))
        if not ag:
            return False
        # Сохраняем дельту. Для вычитания укажем отрицательное число снаружи.
        session.add(AgentAdjustment(tg_id=ag.tg_id, date=date_str, delta=int(delta), reason=reason or ""))
        await session.commit()
        return True


# ===== Вспомогательные вычисления =====
async def count_day(current_date):
    async with async_session() as session:
        # Сумма диалогов
        total_dialogs = await session.scalar(
            select(func.coalesce(func.sum(DailyMessage.dialogs_count), 0)).where(DailyMessage.date == current_date)
        )
        # Сумма корректировок
        total_adj = await session.scalar(
            select(func.coalesce(func.sum(AgentAdjustment.delta), 0)).where(AgentAdjustment.date == current_date)
        )
        return int(total_dialogs or 0) + int(total_adj or 0)


async def all_daily_messages(current_date):
    async with async_session() as session:
        res = []
        messages = await session.scalars(select(DailyMessage).where(DailyMessage.date == current_date))
        for message in messages:
            agent = await session.scalar(select(Agent).where(Agent.tg_id == message.tg_id))
            if message.dialogs_count % 10 == 1 and message.dialogs_count % 100 != 11:
                soo = 'сообщение'
            elif 2 <= message.dialogs_count % 10 <= 4 and not (12 <= message.dialogs_count % 100 <= 14):
                soo = 'сообщения'
            else:
                soo = "сообщений"
            res.append(f'Агент @{agent.nickname} - {message.dialogs_count} {soo}')
        return res


# ===== Управление группами агентов =====
async def ensure_group(title: str):
    async with async_session() as session:
        grp = await session.scalar(select(AgentGroup).where(AgentGroup.title == title))
        if not grp:
            grp = AgentGroup(title=title)
            session.add(grp)
            await session.commit()
            await session.refresh(grp)
        return grp


async def add_member_to_group(title: str, agent_nickname: str):
    async with async_session() as session:
        grp = await session.scalar(select(AgentGroup).where(AgentGroup.title == title))
        if not grp:
            grp = AgentGroup(title=title)
            session.add(grp)
            await session.commit()
            await session.refresh(grp)
        exists = await session.scalar(
            select(AgentGroupMember).where(
                AgentGroupMember.group_id == grp.id,
                AgentGroupMember.agent_nickname == agent_nickname
            )
        )
        if not exists:
            session.add(AgentGroupMember(group_id=grp.id, agent_nickname=agent_nickname))
            await session.commit()


async def list_groups():
    async with async_session() as session:
        result = {}
        groups = await session.scalars(select(AgentGroup))
        for g in groups:
            members = await session.scalars(select(AgentGroupMember).where(AgentGroupMember.group_id == g.id))
            result[g.title] = [m.agent_nickname for m in members]
        return result


# ===== Бизнес-логика: отчёты по группам =====
async def add_dialog(agent_username, client, current_date):
    async with async_session() as session:
        try:
            agent = await session.scalar(select(Agent).where(Agent.nickname == agent_username))
            if not agent:
                return 'not_agent'
            daily_message = await session.scalar(select(DailyMessage)
                                                 .where(DailyMessage.tg_id == agent.tg_id,
                                                        DailyMessage.date == current_date))
            client_obj = await session.scalar(select(Client).where(Client.username == client))
            if not client_obj:
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
            print(f"Ошибка в add_dialog: {e}")
            await session.rollback()


async def delete_dialog(agent_username, client, current_date):
    async with async_session() as session:
        agent = await session.scalar(select(Agent).where(Agent.nickname == agent_username))
        daily_message = await session.scalar(select(DailyMessage)
                                             .where(DailyMessage.tg_id == agent.tg_id,
                                                    DailyMessage.date == current_date))
        if daily_message:
            await session.execute(update(DailyMessage)
                                  .where(DailyMessage.tg_id == agent.tg_id, DailyMessage.date == current_date)
                                  .values(dialogs_count=max(daily_message.dialogs_count - 1, 0))
                                  )
        await session.execute(delete(Client).where(Client.username == client))
        await session.commit()


async def set_agent(from_user):
    async with async_session() as session:
        agent = await session.scalar(select(Agent).where(Agent.tg_id == from_user.id))
        if not agent:
            session.add(Agent(tg_id=from_user.id, nickname=from_user.username, norm_rate=0))
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


# ===== Общий расчёт по периодам с учётом норм и корректировок =====
async def _group_totals_with_rates(session, start_str: str, end_str: str):
    st = await _get_settings(session)
    # Суммы диалогов по агентам
    dialogs_sub = (
        select(
            DailyMessage.tg_id.label('tg_id'),
            func.coalesce(func.sum(DailyMessage.dialogs_count), 0).label('dlg')
        )
        .where(DailyMessage.date.between(start_str, end_str))
        .group_by(DailyMessage.tg_id)
        .subquery()
    )
    # Суммы корректировок по агентам
    adj_sub = (
        select(
            AgentAdjustment.tg_id.label('tg_id'),
            func.coalesce(func.sum(AgentAdjustment.delta), 0).label('adj')
        )
        .where(AgentAdjustment.date.between(start_str, end_str))
        .group_by(AgentAdjustment.tg_id)
        .subquery()
    )

    coalesced_group = func.coalesce(AgentGroup.title, Agent.nickname)

    # Данные по каждому агенту
    rows = (
        await session.execute(
            select(
                Agent.tg_id,
                Agent.nickname,
                Agent.norm_rate,
                coalesced_group.label('group_title'),
                func.coalesce(dialogs_sub.c.dlg, 0).label('dialogs'),
                func.coalesce(adj_sub.c.adj, 0).label('adjust')
            )
            .outerjoin(dialogs_sub, dialogs_sub.c.tg_id == Agent.tg_id)
            .outerjoin(adj_sub, adj_sub.c.tg_id == Agent.tg_id)
            .outerjoin(AgentGroupMember, AgentGroupMember.agent_nickname == Agent.nickname)
            .outerjoin(AgentGroup, AgentGroup.id == AgentGroupMember.group_id)
        )
    ).all()

    # Агрегируем по группам с применением ставок
    by_group = {}
    for tg_id, nickname, agent_norm, group_title, dlg, adj in rows:
        total_dialogs = max(0, int(dlg or 0) + int(adj or 0))
        # Определяем ставку
        if st.norms_enabled or (agent_norm and int(agent_norm) > 0):
            rate = int(agent_norm) if agent_norm and int(agent_norm) > 0 else int(st.global_norm_rate)
        else:
            rate = int(st.rate_per_dialog)
        salary = total_dialogs * rate
        key = group_title or nickname
        if key not in by_group:
            by_group[key] = {'dialogs': 0, 'salary': 0}
        by_group[key]['dialogs'] += total_dialogs
        by_group[key]['salary'] += salary

    # Приводим к итоговому виде
    result = {}
    for title, vals in by_group.items():
        result[title] = [title, int(vals['dialogs']), int(vals['salary'])]
    return result


async def daily_results(current_date):
    dct = {}
    async with async_session() as session:
        try:
            dct = await _group_totals_with_rates(session, current_date, current_date)
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


async def get_biweek_date_range(current_date: datetime.date):
    monday_this, sunday_this = await get_week_date_range(current_date)
    monday_prev = monday_this - timedelta(days=7)
    return monday_prev, sunday_this


async def weekly_results():
    msk_tz = pytz.timezone("Europe/Moscow")
    current_date = datetime.now(msk_tz).date()

    # Отчёт формируем только по воскресеньям (0=Пн … 6=Вс)
    if current_date.weekday() != 6:
        return ""

    monday, sunday = await get_week_date_range(current_date)
    monday_str = monday.strftime("%Y-%m-%d")
    sunday_str = sunday.strftime("%Y-%m-%d")
    period_human = f"{monday.strftime('%d.%m')} - {sunday.strftime('%d.%m')}"

    async with async_session() as session:
        try:
            rows = await _group_totals_with_rates(session, monday_str, sunday_str)
            report_lines = [f"<b>ПРОМЕЖУТОЧНЫЙ НЕДЕЛЬНЫЙ ОТЧЁТ ПО ДИАЛОГАМ</b> ({period_human}):"]
            if not rows:
                report_lines.append("Нет данных за указанный период.")
            else:
                for group_title, (title, dialogs, salary) in rows.items():
                    text = (
                        f"Агент: {title}\n"
                        f"Диалогов за неделю: {dialogs}\n"
                        f"Зарплата за неделю: {salary} рублей"
                    )
                    report_lines.append(text)
            return "\n\n".join(report_lines)
        except Exception as e:
            await session.rollback()
            print(f"Ошибка в weekly_results: {e}")
            return f"Ошибка при формировании отчета: {e}"


async def biweekly_results():
    msk_tz = pytz.timezone("Europe/Moscow")
    current_date = datetime.now(msk_tz).date()

    # Формируем по воскресеньям
    if current_date.weekday() != 6:
        return ""

    start, end = await get_biweek_date_range(current_date)
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")
    period_human = f"{start.strftime('%d.%m')} - {end.strftime('%d.%m')}"

    async with async_session() as session:
        try:
            st = await _get_settings(session)
            rows = await _group_totals_with_rates(session, start_str, end_str)

            # Определяем победителя по диалогам
            winner = None
            winner_dialogs = -1
            for title, (_, dialogs, _) in rows.items():
                if dialogs > winner_dialogs:
                    winner = title
                    winner_dialogs = dialogs

            report_lines = [f"<b>ИТОГОВЫЙ ОТЧЁТ ЗА 2 НЕДЕЛИ</b> ({period_human}):"]
            if not rows:
                report_lines.append("Нет данных за указанный период.")
            else:
                for title, (_, dialogs, salary) in rows.items():
                    total_salary = salary
                    if winner and title == winner and int(st.top_bonus) > 0:
                        total_salary = salary + int(st.top_bonus)
                        text = (
                            f"Агент: {title}\n"
                            f"Диалогов за 2 недели: {dialogs}\n"
                            f"Зарплата за 2 недели: {total_salary} рублей (включая премию {int(st.top_bonus)} рублей)"
                        )
                    else:
                        text = (
                            f"Агент: {title}\n"
                            f"Диалогов за 2 недели: {dialogs}\n"
                            f"Зарплата за 2 недели: {total_salary} рублей"
                        )
                    report_lines.append(text)
                if winner:
                    report_lines.append(
                        f"Премия за наибольшее количество диалогов: {int(st.top_bonus)} рублей — {winner}"
                    )
            return "\n\n".join(report_lines)
        except Exception as e:
            await session.rollback()
            print(f"Ошибка в biweekly_results: {e}")
            return f"Ошибка при формировании отчета: {e}"
