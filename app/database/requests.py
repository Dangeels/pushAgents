import pytz
from datetime import datetime, timedelta, date
from sqlalchemy import select, update, delete, func, case, and_, literal
from app.database.models import async_session, Agent, DailyMessage, Client, Admin, Norm, AgentAccount


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
            # Список аккаунтов агента
            accounts = await session.scalars(select(AgentAccount).where(AgentAccount.agent_id == agent.id))
            primary_id = agent.tg_id
            acc_list = []
            for acc in accounts:
                tag = f"@{acc.tg_username}" if acc.tg_username else (str(acc.tg_id) if acc.tg_id else "&lt;без username&gt;")
                if acc.tg_id and primary_id and acc.tg_id == primary_id:
                    tag = f"{tag} (primary)"
                acc_list.append(tag)
            acc_str = (", ".join(acc_list)) or "-"
            res.append(f'Агент: @{agent.nickname}\nАккаунты: {acc_str}\nТекущая норма: {agent.norm_rate}')
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
        # Суммируем по агенту через join к AgentAccount
        stmt = (
            select(Agent.nickname, func.coalesce(func.sum(DailyMessage.dialogs_count), 0))
            .select_from(DailyMessage)
            .join(AgentAccount, DailyMessage.tg_id == AgentAccount.tg_id)
            .join(Agent, AgentAccount.agent_id == Agent.id)
            .where(DailyMessage.date == literal(current_date))
            .group_by(Agent.id, Agent.nickname)
        )
        rows = (await session.execute(stmt)).all()
        for nickname, dialogs_count in rows:
            dcount = int(dialogs_count or 0)
            if dcount % 10 == 1:
                soo = 'сообщение'
            elif 2 <= dcount % 10 <= 4:
                soo = 'сообщения'
            else:
                soo = "сообщений"
            res.append(f'Агент @{nickname} - {dcount} {soo}')
        return res


async def get_norm():
    async with async_session() as session:
        norm = await session.scalar(select(Norm).where(Norm.id == 1))
        if not norm:
            norm = Norm(id=1, norm=15, salary=0, bonuses=0, week_norm_bonuses=0,
                        best_week_agent=300, norms_enabled_global=1, dialog_price=20)
            session.add(norm)
            await session.commit()
        return norm


async def set_dialog_price(price: int):
    async with async_session() as session:
        norm = await session.scalar(select(Norm).where(Norm.id == 1))
        if not norm:
            norm = Norm(id=1, norm=15, salary=0, bonuses=0, week_norm_bonuses=0,
                        best_week_agent=300, norms_enabled_global=1, dialog_price=price)
        else:
            norm.dialog_price = price
            session.add(norm)
        await session.commit()


async def set_top_premium(amount: int):
    async with async_session() as session:
        norm = await session.scalar(select(Norm).where(Norm.id == 1))
        if not norm:
            norm = Norm(id=1, norm=15, salary=0, bonuses=0, week_norm_bonuses=0,
                        best_week_agent=amount, norms_enabled_global=1, dialog_price=20)
        else:
            norm.best_week_agent = amount
            session.add(norm)
        await session.commit()


async def set_agent_norms(username: str, enabled: bool):
    async with async_session() as session:
        username = username.strip('@')
        await session.execute(update(Agent).where(Agent.nickname == username).values(norms_enabled=1 if enabled else 0))
        await session.commit()


async def set_global_norms(enabled: bool):
    async with async_session() as session:
        norm = await session.scalar(select(Norm).where(Norm.id == 1))
        if not norm:
            norm = Norm(id=1, norm=15, salary=0, bonuses=0, week_norm_bonuses=0, best_week_agent=0, norms_enabled_global=1 if enabled else 0)
            session.add(norm)
        else:
            norm.norms_enabled_global = 1 if enabled else 0
            session.add(norm)
        await session.commit()


async def set_new_norm(
        new_norm: int,
        salary: int | None = None,
        bonuses: int | None = None,
        weekly_bonuses: int | None = None,
        best_week_bonus: int | None = None
):
    async with async_session() as session:
        async with session.begin():
            existing = await session.scalar(select(Norm).where(Norm.id == 1).with_for_update())
            if existing:
                existing.norm = new_norm
                if salary is not None:
                    existing.salary = salary
                if bonuses is not None:
                    existing.bonuses = bonuses
                if weekly_bonuses is not None:
                    existing.week_norm_bonuses = weekly_bonuses
                if best_week_bonus is not None:
                    existing.best_week_agent = best_week_bonus
                session.add(existing)
            else:
                session.add(Norm(id=1, norm=new_norm, salary=salary or 0, bonuses=bonuses or 0,
                                 week_norm_bonuses=weekly_bonuses or 0, best_week_agent=best_week_bonus or 0))

        await session.commit()


async def reset_norm(username, norm):
    async with async_session() as session:
        await session.execute(update(Agent).where(Agent.nickname == username)
                              .values(norm_rate=norm))
        await session.commit()


async def _get_agent_by_username(session, username: str) -> Agent | None:
    username = username.strip('@')
    return await session.scalar(select(Agent).where(Agent.nickname == username))


async def _get_account_by_username(session, username: str) -> AgentAccount | None:
    username = username.strip('@')
    return await session.scalar(select(AgentAccount).where(AgentAccount.tg_username == username))


async def _sync_nickname_with_primary(session, agent_id: int):
    agent = await session.scalar(select(Agent).where(Agent.id == agent_id))
    if not agent or not agent.tg_id:
        return
    acc = await session.scalar(select(AgentAccount).where(AgentAccount.tg_id == agent.tg_id))
    if acc and acc.tg_username and agent.nickname != acc.tg_username:
        await session.execute(
            update(Agent).where(Agent.id == agent.id).values(nickname=acc.tg_username)
        )


async def _sync_all_nicknames(session):
    agents = await session.scalars(select(Agent).where(Agent.tg_id.is_not(None)))
    for ag in agents:
        acc = await session.scalar(select(AgentAccount).where(AgentAccount.tg_id == ag.tg_id))
        if acc and acc.tg_username and ag.nickname != acc.tg_username:
            await session.execute(update(Agent).where(Agent.id == ag.id).values(nickname=acc.tg_username))


async def link_account(agent_username: str, account_username: str):
    async with async_session() as session:
        try:
            agent = await _get_agent_by_username(session, agent_username)
            if not agent:
                return False, f"Агент @{agent_username.strip('@')} не найден"

            acc = await _get_account_by_username(session, account_username)
            # Если есть заглушка/аккаунт по username — переносим
            donor_agent_id = None
            if acc:
                donor_agent_id = acc.agent_id
                # Уже привязан к этому же агенту
                if donor_agent_id == agent.id:
                    # На всякий случай синхронизируем ник по текущему primary
                    await _sync_nickname_with_primary(session, agent.id)
                    await _sync_all_nicknames(session)
                    await session.commit()
                    return True, f"Аккаунт @{account_username.strip('@')} уже привязан к агенту @{agent.nickname}"

                # Переносим аккаунт
                acc.agent_id = agent.id
                session.add(acc)

                # Снимем primary у донора, если он указывал на этот tg_id
                if donor_agent_id and acc.tg_id:
                    donor = await session.scalar(select(Agent).where(Agent.id == donor_agent_id))
                    if donor and donor.tg_id == acc.tg_id:
                        replacement = await session.scalar(
                            select(AgentAccount)
                            .where(AgentAccount.agent_id == donor.id, AgentAccount.id != acc.id, AgentAccount.tg_id.is_not(None))
                            .order_by(AgentAccount.id.asc())
                        )
                        if replacement:
                            await session.execute(
                                update(Agent)
                                .where(Agent.id == donor.id)
                                .values(tg_id=replacement.tg_id,
                                        nickname=(replacement.tg_username if replacement.tg_username else donor.nickname))
                            )
                        else:
                            await session.execute(update(Agent).where(Agent.id == donor.id).values(tg_id=None))

                # Если у целевого агента нет primary и у аккаунта есть tg_id — назначим его и обновим ник
                if not agent.tg_id and acc.tg_id:
                    await session.execute(
                        update(Agent)
                        .where(Agent.id == agent.id)
                        .values(tg_id=acc.tg_id, nickname=(acc.tg_username if acc.tg_username else agent.nickname))
                    )

                # Если у донора не осталось аккаунтов — удалим запись агента
                if donor_agent_id and donor_agent_id != agent.id:
                    cnt = await session.scalar(
                        select(func.count()).select_from(AgentAccount).where(AgentAccount.agent_id == donor_agent_id)
                    )
                    if cnt == 0:
                        await session.execute(delete(Agent).where(Agent.id == donor_agent_id))

                # В конце — синхронизируем никнеймы по текущему primary
                await _sync_nickname_with_primary(session, agent.id)
                if donor_agent_id:
                    await _sync_nickname_with_primary(session, donor_agent_id)
                await _sync_all_nicknames(session)

                await session.commit()
                return True, f"Аккаунт @{account_username.strip('@')} привязан к агенту @{agent.nickname}"

            # Если аккаунта ещё нет — создадим заглушку по username
            session.add(AgentAccount(agent_id=agent.id, tg_id=None, tg_username=account_username.strip('@')))
            # Синхронизируем ник целевого по его текущему primary (не меняется)
            await _sync_nickname_with_primary(session, agent.id)
            await _sync_all_nicknames(session)
            await session.commit()
            return True, f"Аккаунт @{account_username.strip('@')} добавлен к агенту @{agent.nickname} (tg_id появится после первого отчёта)"
        except Exception as e:
            await session.rollback()
            return False, f"Ошибка привязки аккаунта: {e}"


async def list_accounts(agent_username: str):
    async with async_session() as session:
        agent = await _get_agent_by_username(session, agent_username)
        if not agent:
            return []
        accs = await session.scalars(select(AgentAccount).where(AgentAccount.agent_id == agent.id))
        out = []
        for a in accs:
            label = f"@{a.tg_username}" if a.tg_username else (str(a.tg_id) if a.tg_id else "&lt;без username&gt;")
            if a.tg_id and agent.tg_id and a.tg_id == agent.tg_id:
                label = f"{label} (primary)"
            out.append(label)
        return out


async def unlink_account(agent_username: str, account_username: str):
    async with async_session() as session:
        try:
            agent = await _get_agent_by_username(session, agent_username)
            if not agent:
                return False, f"Агент @{agent_username.strip('@')} не найден"

            acc = await _get_account_by_username(session, account_username)
            if not acc:
                return False, f"Аккаунт @{account_username.strip('@')} не найден"

            if acc.agent_id != agent.id:
                return False, f"Аккаунт @{account_username.strip('@')} не привязан к агенту @{agent.nickname}"

            # Если отвязываем primary — переключим primary на другой аккаунт агента, если доступен
            if acc.tg_id and agent.tg_id and acc.tg_id == agent.tg_id:
                replacement = await session.scalar(
                    select(AgentAccount)
                    .where(AgentAccount.agent_id == agent.id, AgentAccount.id != acc.id, AgentAccount.tg_id.is_not(None))
                    .order_by(AgentAccount.id.asc())
                )
                if replacement:
                    await session.execute(
                        update(Agent)
                        .where(Agent.id == agent.id)
                        .values(tg_id=replacement.tg_id,
                                nickname=(replacement.tg_username if replacement.tg_username else agent.nickname))
                    )
                else:
                    await session.execute(update(Agent).where(Agent.id == agent.id).values(tg_id=None))

            # Удаляем связь
            await session.execute(delete(AgentAccount).where(AgentAccount.id == acc.id))

            # Если это был последний аккаунт — удаляем и самого агента
            remaining = await session.scalar(
                select(func.count()).select_from(AgentAccount).where(AgentAccount.agent_id == agent.id)
            )
            agent_deleted_note = ""
            if remaining == 0:
                await session.execute(delete(Agent).where(Agent.id == agent.id))
                agent_deleted_note = "; агент удалён"
            else:
                # Синхронизируем ник по текущему primary
                await _sync_nickname_with_primary(session, agent.id)

            await _sync_all_nicknames(session)
            await session.commit()
            if agent_deleted_note:
                return True, f"Аккаунт @{account_username.strip('@')} отвязан от агента @{agent.nickname}{agent_deleted_note}"
            else:
                return True, f"Аккаунт @{account_username.strip('@')} отвязан от агента @{agent.nickname}"
        except Exception as e:
            await session.rollback()
            return False, f"Ошибка отвязки аккаунта: {e}"


async def set_primary_account(agent_username: str, account_username: str):
    """Устанавливает primary-аккаунт агента.
    1) Гарантирует, что аккаунт привязан к указанному агенту (переносит при необходимости)
    2) Требует, чтобы у аккаунта был tg_id (иначе сообщает об ошибке)
    3) Устанавливает agent.tg_id = acc.tg_id и обновляет agent.nickname на tg_username primary
    """
    # Сначала гарантируем привязку (в т.ч. перенос между агентами)
    ok, _msg = await link_account(agent_username, account_username)
    if not ok:
        return False, _msg

    async with async_session() as session:
        try:
            agent = await _get_agent_by_username(session, agent_username)
            if not agent:
                return False, f"Агент @{agent_username.strip('@')} не найден"

            acc = await _get_account_by_username(session, account_username)
            if not acc:
                return False, f"Аккаунт @{account_username.strip('@')} не найден"

            if acc.agent_id != agent.id:
                return False, f"Аккаунт @{account_username.strip('@')} не привязан к агенту @{agent.nickname}"

            if not acc.tg_id:
                return False, f"У аккаунта @{account_username.strip('@')} нет tg_id. Нельзя назначить primary до первого отчёта."

            # Устанавливаем primary и ник агента в соответствии с username аккаунта
            await session.execute(
                update(Agent)
                .where(Agent.id == agent.id)
                .values(tg_id=acc.tg_id, nickname=(acc.tg_username if acc.tg_username else agent.nickname))
            )

            # На всякий случай — глобальная синхронизация никнеймов по текущему primary
            await _sync_all_nicknames(session)

            await session.commit()
            return True, f"Primary-аккаунт агента @{acc.tg_username or agent.nickname} установлен: @{account_username.strip('@')}"
        except Exception as e:
            await session.rollback()
            return False, f"Ошибка установки primary: {e}"


async def add_dialog(agent_username, client, current_date):
    async with async_session() as session:
        try:
            # Добавляем диалог к аккаунту агента
            agent = await session.scalar(select(Agent).where(Agent.nickname == agent_username))
            if not agent:
                return 'not_agent'

            # выбираем primary tg_id агента или любой доступный аккаунт с tg_id
            primary_tg_id = agent.tg_id
            if not primary_tg_id:
                replacement = await session.scalar(
                    select(AgentAccount.tg_id)
                    .where(AgentAccount.agent_id == agent.id, AgentAccount.tg_id.is_not(None))
                    .order_by(AgentAccount.id.asc())
                )
                if replacement:
                    primary_tg_id = replacement
                else:
                    return 'not_agent'

            daily_message = await session.scalar(select(DailyMessage)
                                                 .where(DailyMessage.tg_id == primary_tg_id,
                                                        DailyMessage.date == current_date))
            client_row = await session.scalar(select(Client).where(Client.username == client))
            if not client_row:
                session.add(Client(username=client))
            else:
                return 'повтор'
            if not daily_message:
                session.add(DailyMessage(tg_id=primary_tg_id, date=current_date, dialogs_count=1))
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
        if not agent:
            return
        # Уменьшаем на основном аккаунте
        daily_message = await session.scalar(select(DailyMessage)
                                             .where(DailyMessage.tg_id == agent.tg_id,
                                                    DailyMessage.date == literal(current_date)))
        if daily_message:
            new_count = daily_message.dialogs_count - 1
            if new_count < 0:
                new_count = 0
            await session.execute(update(DailyMessage)
                                  .where(DailyMessage.tg_id == agent.tg_id, DailyMessage.date == literal(current_date))
                                  .values(dialogs_count=new_count)
                                  )
        await session.execute(delete(Client).where(Client.username == client))
        await session.commit()


async def set_agent(from_user):
    async with async_session() as session:
        # Если уже есть привязка аккаунта — ничего не создаём, только сверим username
        acc = await session.scalar(select(AgentAccount).where(AgentAccount.tg_id == from_user.id))
        if acc:
            # обновим отображаемый ник
            if from_user.username and acc.tg_username != from_user.username:
                acc.tg_username = from_user.username
                session.add(acc)
            await session.commit()
            return

        # Если есть предварительная запись по username — привяжем tg_id
        if from_user.username:
            acc_by_username = await session.scalar(select(AgentAccount).where(AgentAccount.tg_username == from_user.username, AgentAccount.tg_id.is_(None)))
            if acc_by_username:
                acc_by_username.tg_id = from_user.id
                session.add(acc_by_username)
                await session.commit()
                return

        # Иначе создаём нового агента и привязку
        norm = await get_norm()
        new_agent = Agent(tg_id=from_user.id, nickname=from_user.username or str(from_user.id), norm_rate=norm.norm)
        session.add(new_agent)
        await session.flush()
        session.add(AgentAccount(agent_id=new_agent.id, tg_id=from_user.id, tg_username=from_user.username))
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
            # Суммарные диалоги по агентам за день + статус норм
            stmt = (
                select(
                    Agent.id,
                    Agent.nickname,
                    Agent.norm_rate,
                    Agent.norms_enabled,
                    func.coalesce(func.sum(DailyMessage.dialogs_count), 0).label('total')
                )
                .select_from(Agent)
                .join(AgentAccount, Agent.id == AgentAccount.agent_id)
                .join(DailyMessage, DailyMessage.tg_id == AgentAccount.tg_id)
                .where(DailyMessage.date == literal(current_date))
                .group_by(Agent.id, Agent.nickname, Agent.norm_rate, Agent.norms_enabled)
            )
            rows = (await session.execute(stmt)).all()

            all_norms = await get_norm()
            norms_global = int(all_norms.norms_enabled_global or 0)

            for agent_id, nickname, old_norm, agent_norms_enabled, total in rows:
                bonuses = 0
                # Если нормы отключены глобально или у агента — зарплата = диалоги * dialog_price, норму не трогаем
                if norms_global == 0 or int(agent_norms_enabled or 0) == 0:
                    salary = int(total) * int(all_norms.dialog_price or 20)
                    new_norm = old_norm
                else:
                    if total >= old_norm:
                        bonuses = all_norms.bonuses * int((total - old_norm) / 5)
                        salary = all_norms.salary + bonuses
                        new_norm = all_norms.norm - (total - old_norm) % 5
                    else:
                        salary = 0
                        new_norm = old_norm - total

                # обновим норму агента (без изменений в режиме отключённых норм)
                await session.execute(update(Agent).where(Agent.id == agent_id).values(norm_rate=new_norm))

                # распределим зарплату: только в одну строку DailyMessage — предпочтительно primary tg_id
                primary_tg_id = await session.scalar(select(Agent.tg_id).where(Agent.id == agent_id))
                # обнулим salary во всех строках агента за день
                await session.execute(update(DailyMessage)
                                      .where(and_(DailyMessage.date == literal(current_date),
                                                  DailyMessage.tg_id.in_(select(AgentAccount.tg_id).where(AgentAccount.agent_id == agent_id))))
                                      .values(salary=0))
                # ставим зарплату в нужную строку
                dm_primary = await session.scalar(select(DailyMessage).where(DailyMessage.tg_id == primary_tg_id, DailyMessage.date == literal(current_date)))
                if dm_primary:
                    await session.execute(update(DailyMessage)
                                          .where(DailyMessage.tg_id == primary_tg_id, DailyMessage.date == literal(current_date))
                                          .values(salary=salary))
                else:
                    any_dm = await session.scalar(
                        select(DailyMessage)
                        .where(and_(DailyMessage.date == literal(current_date),
                                    DailyMessage.tg_id.in_(select(AgentAccount.tg_id).where(AgentAccount.agent_id == agent_id))))
                        .order_by(DailyMessage.dialogs_count.desc())
                    )
                    if any_dm:
                        await session.execute(update(DailyMessage)
                                              .where(DailyMessage.tg_id == any_dm.tg_id, DailyMessage.date == literal(current_date))
                                              .values(salary=salary))

                dct[agent_id] = [nickname, int(total), int(bonuses), int(salary), int(old_norm), int(new_norm)]

            await session.commit()
            return dct
        except Exception as e:
            print(f"Ошибка в daily_results: {e}")
            await session.rollback()
            return {}


async def get_week_date_range(current_date: date):
    days_since_monday = current_date.weekday()
    monday = current_date - timedelta(days=days_since_monday)
    sunday = monday + timedelta(days=6)
    return monday, sunday


async def weekly_results():
    msk_tz = pytz.timezone("Europe/Moscow")
    current_date = datetime.now(msk_tz).date()

    # Отчёт формируем только по воскресеньям (0=Пн … 6=Вс)
    if current_date.weekday() != 6:
        return ""

    # Чередование: недельный отчёт публикуем только в нечётные ISO-недели
    iso_week = current_date.isocalendar()[1]
    if iso_week % 2 == 0:
        return ""

    monday, sunday = await get_week_date_range(current_date)

    monday_str = monday.strftime("%Y-%m-%d")
    sunday_str = sunday.strftime("%Y-%m-%d")

    period_human = f"{monday.strftime('%d.%m')} - {sunday.strftime('%d.%m')}"

    async with async_session() as session:
        try:
            stmt = (
                select(
                    Agent.id,
                    Agent.nickname,
                    func.coalesce(func.sum(DailyMessage.dialogs_count), 0).label("total_dialogs"),
                    func.coalesce(func.sum(DailyMessage.salary), 0).label("total_salary"),
                    func.coalesce(
                        func.sum(
                            case((DailyMessage.salary > 0, 1), else_=0)
                        ),
                        0
                    ).label("positive_days"),
                )
                .select_from(Agent)
                .join(AgentAccount, Agent.id == AgentAccount.agent_id)
                .join(DailyMessage, DailyMessage.tg_id == AgentAccount.tg_id)
                .where(DailyMessage.date.between(monday_str, sunday_str))
                .group_by(Agent.id, Agent.nickname)
            )

            result = await session.execute(stmt)
            rows = result.all()

            report_lines = ["<b>ПРОМЕЖУТОЧНЫЙ НЕДЕЛЬНЫЙ ОТЧЁТ ПО ДИАЛОГАМ</b>", f"Отчёт за неделю ({period_human}):"]

            norm = await get_norm()

            if not rows:
                report_lines.append("Нет данных за указанный период.")
            else:
                for agent_id, nickname, total_dialogs, total_salary, positive_days in rows:
                    text = (
                        f"Агент: @{nickname}\n"
                        f"Диалогов за неделю: {total_dialogs}\n"
                        f"Зарплата за неделю за диалоги: {total_salary} рублей"
                    )

                    if positive_days == 7:
                        text += f"\nБонус +{norm.week_norm_bonuses} рублей за ежедневное выполнение нормы"
                        total_salary += norm.week_norm_bonuses

                    report_lines.append(text)

            return "\n\n".join(report_lines)

        except Exception as e:
            await session.rollback()
            print(f"Ошибка в weekly_results: {e}")
            return f"Ошибка при формировании отчета: {e}"


async def biweekly_results():
    """Итоговый отчёт за 2 прошедшие недели (с понедельника двухнедельного периода по воскресенье включительно)."""
    msk_tz = pytz.timezone("Europe/Moscow")
    current_date = datetime.now(msk_tz).date()

    # Формируем по воскресеньям
    if current_date.weekday() != 6:
        return ""

    # Чередование: двухнедельный отчёт публикуем только в чётные ISO-недели
    iso_week = current_date.isocalendar()[1]
    if iso_week % 2 == 1:
        return ""

    # Две прошедшие недели: от понедельника 2 недели назад до сегодняшнего воскресенья
    # Для воскресенья weekday()=6 => старт = current_date - 13
    start = current_date - timedelta(days=current_date.weekday() + 7)
    end = current_date

    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    period_human = f"{start.strftime('%d.%m')} - {end.strftime('%d.%m')}"

    async with async_session() as session:
        try:
            stmt = (
                select(
                    Agent.id,
                    Agent.nickname,
                    func.coalesce(func.sum(DailyMessage.dialogs_count), 0).label("total_dialogs"),
                    func.coalesce(func.sum(DailyMessage.salary), 0).label("total_salary"),
                )
                .select_from(Agent)
                .join(AgentAccount, Agent.id == AgentAccount.agent_id)
                .join(DailyMessage, DailyMessage.tg_id == AgentAccount.tg_id)
                .where(DailyMessage.date.between(start_str, end_str))
                .group_by(Agent.id, Agent.nickname)
            )

            rows = (await session.execute(stmt)).all()

            lines = ["<b>ИТОГОВЫЙ ОТЧЁТ ЗА 2 НЕДЕЛИ</b>", f"Период: {period_human}"]

            if not rows:
                lines.append("Нет данных за указанный период.")
            else:
                # Определим топ по диалогам
                top_agent = None  # (nickname, total_dialogs, total_salary)
                for _id, nickname, total_dialogs, total_salary in rows:
                    lines.append(
                        f"Агент: @{nickname}\n"
                        f"Диалогов за 2 недели: {total_dialogs}\n"
                        f"Зарплата за 2 недели за диалоги: {total_salary} рублей"
                    )
                    if (top_agent is None) or (total_dialogs > top_agent[1]):
                        top_agent = (nickname, int(total_dialogs), int(total_salary))

                # Премия за наибольшее количество диалогов за 2 недели
                norm = await get_norm()
                if top_agent and int(norm.best_week_agent or 0) > 0:
                    final_salary = top_agent[2] + int(norm.best_week_agent)
                    lines.append(
                        f"Премия за наибольшее количество диалогов: @{top_agent[0]} +{norm.best_week_agent} рублей\n"
                        f"Итоговая зарплата победителя: {final_salary}"
                    )

            return "\n\n".join(lines)
        except Exception as e:
            await session.rollback()
            print(f"Ошибка в biweekly_results: {e}")
            return f"Ошибка при формировании 2-недельного отчёта: {e}"


async def delete_agent(agent_username: str):
    """Полностью удаляет агента и все его записи в agent_accounts."""
    async with async_session() as session:
        try:
            agent = await _get_agent_by_username(session, agent_username)
            if not agent:
                return False, f"Агент @{agent_username.strip('@')} не найден"
            # Сначала удаляем все аккаунты агента
            await session.execute(delete(AgentAccount).where(AgentAccount.agent_id == agent.id))
            # Затем удаляем самого агента
            await session.execute(delete(Agent).where(Agent.id == agent.id))
            await session.commit()
            return True, f"Агент @{agent.nickname} удалён"
        except Exception as e:
            await session.rollback()
            return False, f"Ошибка удаления агента: {e}"


async def all_accounts():
    """Возвращает список строк по всем аккаунтам из agent_accounts с пометкой primary и именем агента."""
    async with async_session() as session:
        rows = (await session.execute(
            select(AgentAccount, Agent)
            .join(Agent, AgentAccount.agent_id == Agent.id)
            .order_by(Agent.nickname.asc(), AgentAccount.id.asc())
        )).all()
        out: list[str] = []
        for acc, ag in rows:
            label = f"@{acc.tg_username}" if acc.tg_username else (str(acc.tg_id) if acc.tg_id else "&lt;без username&gt;")
            is_primary = bool(acc.tg_id and ag.tg_id and acc.tg_id == ag.tg_id)
            suffix = " (primary)" if is_primary else ""
            out.append(f"{label}{suffix} — агент @{ag.nickname}")
        return out


async def is_agent_norms_enabled(username: str) -> bool:
    async with async_session() as session:
        username = username.strip('@')
        val = await session.scalar(select(Agent.norms_enabled).where(Agent.nickname == username))
        return bool(val) if val is not None else True


async def subtract_dialogs(agent_username: str, current_date: str, amount: int) -> tuple[bool, str, int]:
    """Уменьшает суммарное количество диалогов у агента за указанную дату на amount.
    Не трогает таблицу clients. Распределяет вычитание по строкам DailyMessage агента.
    Возвращает (ok, message, actually_subtracted).
    """
    if amount <= 0:
        return False, "Количество для вычитания должно быть положительным", 0

    async with async_session() as session:
        try:
            agent_username = agent_username.strip('@')
            agent = await session.scalar(select(Agent).where(Agent.nickname == agent_username))
            if not agent:
                return False, f"Агент @{agent_username} не найден", 0

            # Все строки отчётов агента за дату по всем аккаунтам
            dm_rows = await session.scalars(
                select(DailyMessage)
                .join(AgentAccount, DailyMessage.tg_id == AgentAccount.tg_id)
                .where(AgentAccount.agent_id == agent.id, DailyMessage.date == literal(current_date))
                .order_by(DailyMessage.dialogs_count.desc())
            )
            rows = list(dm_rows)
            if not rows:
                return False, "На эту дату у агента нет диалогов", 0

            remaining = int(amount)
            total_sub = 0
            for dm in rows:
                if remaining <= 0:
                    break
                cur = int(dm.dialogs_count or 0)
                if cur <= 0:
                    continue
                delta = min(cur, remaining)
                new_val = cur - delta
                await session.execute(
                    update(DailyMessage)
                    .where(DailyMessage.tg_id == dm.tg_id, DailyMessage.date == dm.date)
                    .values(dialogs_count=new_val)
                )
                remaining -= delta
                total_sub += delta

            await session.commit()
            if total_sub == 0:
                return False, "Нечего вычитать (у агента 0 диалогов)", 0
            return True, f"Вычтено диалогов: {total_sub}", total_sub
        except Exception as e:
            await session.rollback()
            return False, f"Ошибка вычитания диалогов: {e}", 0
