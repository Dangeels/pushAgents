import asyncio
import os
import pytz

from datetime import datetime
from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command
from app.database import requests as req

router = Router()
media_groups = set()


@router.message(Command('all_admins'))
async def all_admins(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if ad[0]:
        admins = await req.all_admins()
        await message.answer('Список администраторов\n'+'\n'.join(admins[0]))


@router.message(Command('delete_admin'))  # формат сообщения /delete_admin @user
async def delete_admin(message: Message):
    if message.chat.type != 'private':
        return
    try:
        username = message.text.split()[1].strip('@')
        ad = await is_admin(message.from_user.username)
        ad2 = await is_admin(username)
        if username == message.from_user.username:
            await message.answer('Вы не можете удалить себя из списка администраторов')
            return
        if ad[0] and ad[2] and ad2[0]:
            dct = {True: 'Пользователь успешно удалён из списка администраторов',
                   False: 'Ошибка доступа: вы не можете удалить этого пользователя'}
            c = await req.delete_admin(message.from_user, username)
            await message.answer(f'{dct[c]}')
        else:
            await message.answer('Пользователь не найден в списке администраторов')
    except Exception:
        await message.answer('Ошибка в формате сообщения')


@router.message(Command('set_client'))  # сообщение формата /set_client @username
async def set_client(message: Message):
    if message.chat.type != 'private':
        return
    try:
        ad = await is_admin(message.from_user.username)
        if ad[0]:
            username = message.text.split()[1].strip('@')
            await req.set_client(username)
            await message.answer(f'@{username} успешно добавлен в список клиентов')
    except Exception:
        await message.answer(f'Ошибка в формате сообщения')


@router.message(Command('set_admin'))  # сообщение формата /set_admin @username
async def set_admin(message: Message):
    if message.chat.type != 'private':
        return
    try:
        ad = await is_admin(message.from_user.username)
        if ad[0] and ad[2]:
            username = message.text.split()[1].strip('@')
            await req.set_admin(username)
            await message.answer(f'@{username} успешно добавлен в список администраторов')
    except Exception:
        await message.answer(f'Ошибка в формате сообщения')


async def is_admin(username):
    x = await req.is_admin(username)
    return x


def get_current_date():
    msk_tz = pytz.timezone("Europe/Moscow")
    now = datetime.now(msk_tz)
    date_str = now.strftime("%Y-%m-%d")
    return date_str


@router.message(Command('help'))
async def bot_help(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if ad[0]:
        txt = """
<b>СПРАВКА по командам</b>

<b>Формат параметров:</b>
- &lt;значение&gt; — обязательный параметр
- [значение] — опциональный параметр

<b>1) Диалоги и корректировки</b>
• /add_dialog @agent @client — добавить 1 диалог агенту за текущий день.
  Пример: /add_dialog @ivan @client123. Клиента добавит в базу, если его там нет; медиа прикреплять не нужно.
• /delete_dialog @agent @client — удалить 1 диалог за текущий день у агента и удалить клиента из базы clients.
  Ограничение: удаляются только диалоги за текущий день.
• /sub_dialogs @agent &lt;N&gt; — вычесть N диалогов у агента за сегодня без удаления клиентов; зарплата пересчитается автоматически.
  Пример: /sub_dialogs @ivan 3.
• /set_client @username — пометить клиента обработанным (повторные упоминания не засчитываются).
• /all_daily_messages — показать список диалогов по агентам за сегодня и их суммарное количество.
• /all_time_messages — показать суммарное количество диалогов за всё время.

<b>2) Нормы и расчёт оплаты</b>
• /set_new_norm &lt;норма&gt; [зарплата] [ежедневный_бонус_за_5] [недельный_бонус_за_норму] [премия_топа]
  — установить базовую дневную норму и опционально зарплату/бонусы.
  Примеры:
    /set_new_norm 25 400 100 400 600 — норма 25, дневная ЗП 400, +100 за каждые 5 сверх нормы, +400 за неделю выполнения, +600 топ-агенту.
    /set_new_norm 30 500 — норма 30, ЗП 500, остальные значения без изменений.
    /set_new_norm 40 — только обновить норму, остальное без изменений.
• /agent_norms @agent on|off — включить или выключить нормы для конкретного агента.
• /norms_global on|off — глобально включить или выключить нормы (доступно только старшим админам).
• /set_dialog_price &lt;стоимость&gt; — установить цену одного диалога, если нормы отключены (по умолчанию 20).
• /set_top_premium &lt;сумма&gt; — установить премию агенту с наибольшим числом диалогов за 2 недели.

<b>3) Агенты и аккаунты</b>
• /all_agents — показать всех агентов, их аккаунты и текущую норму.
• /link_account @agent @account — привязать аккаунт (по username) к агенту. Переносит между агентами при необходимости;
  если у аккаунта ещё нет tg_id, создаётся заглушка до первого отчёта.
• /unlink_account @agent @account — отвязать аккаунт от агента. Если это последний аккаунт, агент будет удалён.
• /set_primary @agent @account — задать primary-аккаунт агента (у аккаунта должен быть tg_id).
• /list_accounts @agent — показать все аккаунты агента (primary помечен).
• /all_accounts — показать все аккаунты всех агентов (primary помечен).
• /delete_agent @agent — полностью удалить агента и все его аккаунты.

<b>4) Администрирование</b>
• /all_admins — показать список администраторов.
• /set_admin @username — добавить администратора.
• /delete_admin @username — удалить администратора (доступ зависит от уровня прав).
"""
        await message.answer(txt)


@router.message(Command('agent_norms'))  # /agent_norms @agent on|off
async def agent_norms(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        _, agent_username, state = message.text.split()
        state = state.lower().strip()
        enabled = state in ['on', '1', 'true', 'вкл', 'enable']
        await req.set_agent_norms(agent_username.strip('@'), enabled)
        await message.answer(f"Нормы для @{agent_username.strip('@')} {'включены' if enabled else 'выключены'}")
    except Exception:
        await message.answer('Неверный формат. Пример: /agent_norms @agent on|off')


@router.message(Command('norms_global'))  # /norms_global on|off
async def norms_global(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0] or ad[2] == 0:
        await message.answer('Доступ запрещён')
        return
    try:
        _, state = message.text.split()
        state = state.lower().strip()
        enabled = state in ['on', '1', 'true', 'вкл', 'enable']
        await req.set_global_norms(enabled)
        await message.answer(f"Глобальные нормы {'включены' if enabled else 'выключены'}")
    except Exception:
        await message.answer('Неверный формат. Пример: /norms_global on|off')


@router.message(Command('all_agents'))
async def all_agents(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    agents = await req.all_agents()
    await message.answer('\n\n'.join(agents[0]))


@router.message(Command('all_accounts'))
async def all_accounts(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    accounts = await req.all_accounts()
    text = '\n'.join(accounts) or 'Нет данных.'
    for i in range(0, len(text), 4096):
        await message.answer(text[i:i+4096])


@router.message(Command('all_time_messages'))
async def all_time_messages(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    count = await req.all_time_messages()
    await message.answer(f'Суммарное количество сообщений за всё время: {count}')


@router.message(Command('all_daily_messages'))
async def all_daily_messages(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    current_date = get_current_date()
    messages = await req.all_daily_messages(current_date)
    count = sum([int(i.split()[-2]) for i in messages])
    await message.answer(f'Сообщения за {current_date}. Количество: {count}\n'+'\n'.join(messages))


@router.message(Command('set_new_norm'))  # /set_new_norm [norm] [salary] [bonuses] [week_bonuses] [best_agent]
async def set_new_norm(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0] or ad[2] == 0:
        await message.answer('Доступ запрещён')
        return
    try:
        x = [int(i) for i in message.text.split()[1:]]
        await req.set_new_norm(*x)
        await message.answer('Норма успешно обновлена')
    except Exception:
        await message.answer(f'Неверный формат сообщения')


@router.message(Command('reset_norm'))  # сообщение формата /reset_norm nickname 15
async def reset_norm(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        username = message.text.split()[1]
        username = username.strip('@')
        if len(message.text.split()) == 3:
            norm = abs(int(message.text.split()[2]))
        else:
            norm = await req.get_norm()
            norm = norm.norm
        agents = await req.all_agents()
        if username in agents[1]:
            await req.reset_norm(username, norm)
            await message.answer(f'Дневная норма @{username} успешно сброшена до {norm}')
        else:
            await message.answer(f'Агента @{username} не существует')
    except Exception:
        await message.answer('Неверный формат сообщения')


@router.message(Command('link_account'))  # /link_account @agent @account
async def link_account(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        _, agent_username, account_username = message.text.split()
        ok, text = await req.link_account(agent_username.strip('@'), account_username.strip('@'))
        await message.answer(text)
    except Exception:
        await message.answer('Неверный формат. Пример: /link_account @agent @account')


@router.message(Command('unlink_account'))  # /unlink_account @agent @account
async def unlink_account(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        _, agent_username, account_username = message.text.split()
        ok, text = await req.unlink_account(agent_username.strip('@'), account_username.strip('@'))
        await message.answer(text)
    except Exception:
        await message.answer('Неверный формат. Пример: /unlink_account @agent @account')


@router.message(Command('set_primary'))  # /set_primary @agent @account
async def set_primary(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        _, agent_username, account_username = message.text.split()
        ok, text = await req.set_primary_account(agent_username.strip('@'), account_username.strip('@'))
        await message.answer(text)
    except Exception:
        await message.answer('Неверный формат. Пример: /set_primary @agent @account')


# Новый обработчик: показать привязанные аккаунты агента
@router.message(Command('list_accounts'))  # /list_accounts @agent
async def list_accounts(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            await message.answer('Неверный формат. Пример: /list_accounts @agent')
            return
        agent_username = parts[1].strip('@')
        accounts = await req.list_accounts(agent_username)
        if not accounts:
            await message.answer(f'Агент @{agent_username} не найден или у него нет привязанных аккаунтов')
            return
        await message.answer('Аккаунты агента @' + agent_username + ' (primary помечен):\n- ' + '\n- '.join(accounts))
    except Exception:
        await message.answer('Неверный формат. Пример: /list_accounts @agent')


@router.message(Command('delete_agent'))  # /delete_agent @agent
async def delete_agent(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        _, agent_username = message.text.split()
        ok, text = await req.delete_agent(agent_username.strip('@'))
        await message.answer(text)
    except Exception:
        await message.answer('Неверный формат. Пример: /delete_agent @agent')


@router.message(Command('sub_dialogs'))  # /sub_dialogs @agent N
async def sub_dialogs(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        _, agent_username, num = message.text.split()
        agent_username = agent_username.strip('@')
        amount = int(num)
        if amount <= 0:
            await message.answer('Число должно быть положительным')
            return
        date_str = get_current_date()
        ok, text, _sub = await req.subtract_dialogs(agent_username, date_str, amount)
        # Пересчитаем зарплату за текущий день после корректировки
        await req.daily_results(date_str)
        await message.answer(text)
    except ValueError:
        await message.answer('Неверный формат. Пример: /sub_dialogs @agent 3')
    except Exception as e:
        await message.answer(f'Ошибка: {e}')


@router.message(F.text.startswith('@'))  # сообщение формата  @username
async def check_client(message: Message):
    try:
        client = message.text.split()[0].strip().strip('@')
        if len(client.split()) != len(message.text.split()):
            return
        clients = await req.all_clients()
        dct = {True: f'@{client} находится в списке клиентов, диалог с ним засчитан не будет',
               False: f'@{client} не найден в списке клиентов, диалог будет засчитан'
               }
        response = await message.answer(dct[client in clients[1]])
        await asyncio.sleep(10)
        await response.delete()
    except Exception as e:
        await message.answer(f'Ошибка в формате сообщения {e}')


@router.message(Command('add_dialog'))  # /add_dialog @agent_nickname @client_nickname
async def add_dialog(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        agent_nickname = message.text.split()[1].strip('@')
        client_nickname = message.text.split()[2].strip('@')
        repeat = await req.add_dialog(agent_nickname, client_nickname, get_current_date())
        if repeat == 'not_agent':
            await message.answer('Этого агента нет в базе данных. Сначала ему требуется отправить хотя бы один отчёт')
        elif repeat == 'повтор':
            await message.answer('Повтор. Этот клиент ранее упоминался в отчёте')
        else:
            await message.answer(f'Диалог успешно добавлен агенту @{agent_nickname}')
    except Exception:
        await message.answer(f'Неверный формат сообщения')


@router.message(Command('delete_dialog'))  # сообщение формата /delete_dialog agent_nickname client_nickname
async def delete_dialog(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        agent_nickname = message.text.split()[1].strip('@')
        client_nickname = message.text.split()[2].strip('@')
        agents = await req.all_agents()
        clients = await req.all_clients()
        if agent_nickname in agents[1] and client_nickname in clients[1]:
            date_str = get_current_date()
            await req.delete_dialog(agent_nickname, client_nickname, date_str)
            ms = await req.all_daily_messages(date_str)
            m = [i for i in ms if agent_nickname in i]
            await message.answer(f'Сообщение успешно удалено\nСообщения агента за {date_str}\n' + ''.join(m))
        else:
            await message.answer('Неверный @username агента или клиента')
    except Exception:
        await message.answer(f'Неверный формат сообщения')


@router.message(F.photo and F.caption and F.caption.count('@'))
async def dialogs_handler(message: Message):
    try:
        # Проверяем, что в подписи ровно одно упоминание (@)
        entities = message.caption_entities or []
        mention_entities = [e for e in entities if e.type in ("mention", "text_mention")]
        if len(mention_entities) != 1:
            await message.answer("Сообщение должно содержать ровно одно упоминание (@).")
            return

        # Проверяем, что подпись состоит ТОЛЬКО из упоминания
        mention = mention_entities[0].length
        mention_text = message.caption.strip()  # Получаем текст упоминания
        if mention != len(mention_text):
            await message.answer(
                "Сообщение должно содержать только одно упоминание (@username) без дополнительного текста.")
            return

        date_str = get_current_date()

        await req.set_agent(message.from_user)
        repeat = await req.count_daily_messages(message.from_user, date_str, message)

        if repeat:
            await message.answer('Повтор. Этот клиент ранее упоминался в отчёте')

        media = message.media_group_id
        if media and media not in media_groups:
            media_groups.add(media)
            asyncio.create_task(clean_processed_media_groups(media))

    except Exception as e:
        print(f"Ошибка в обработке сообщения: {e}")
        await message.answer("Произошла ошибка при обработке сообщения.")


async def clean_processed_media_groups(media_group_id):
    await asyncio.sleep(10)
    media_groups.discard(media_group_id)


async def day_res(bot):
    date_str = get_current_date()
    dct = await req.daily_results(date_str)
    # Получаем глобальные настройки норм (зарплата/бонусы/включены ли нормы)
    norm_cfg = await req.get_norm()
    res = []
    done = {True: '(норма выполнена)', False: '(норма не выполнена)'}
    for key in dct.keys():
        # dct[key] = [nickname, total, bonuses, salary, old_norm, new_norm]
        nickname = dct[key][0]
        total = dct[key][1]
        bonuses = dct[key][2]
        salary = dct[key][3]
        old_norm = dct[key][4]
        new_norm = dct[key][5]
        if total == 0:
            continue

        # Проверяем включены ли нормы глобально и у конкретного агента
        try:
            agent_norm_enabled = await req.is_agent_norms_enabled(nickname)
        except Exception:
            agent_norm_enabled = True
        norms_active = bool(norm_cfg.norms_enabled_global) and bool(agent_norm_enabled)

        # Перенос диалогов показываем ТОЛЬКО если нормы активны
        perenos_line = ''
        if norms_active:
            if total < old_norm:
                perenos = total
            else:
                perenos = (total - old_norm) % 5
            perenos_line = f"\nПеренос диалогов на завтра: {perenos} (завтрашняя норма {new_norm})"

        # Формула расчёта зарплаты
        if norms_active:
            if total >= old_norm:
                groups = (total - old_norm) // 5
                formula = f"Формула: {norm_cfg.salary} + {norm_cfg.bonuses} × {groups} = {salary}"
            else:
                formula = f"Формула: 0 (норма {old_norm}, выполнено {total})"
        else:
            formula = f"Формула: {norm_cfg.dialog_price} × {total} = {salary}"

        # Строка количества диалогов: без нормы и статуса при отключённых нормах
        if norms_active:
            count_line = f"Количество диалогов за день: {total}/{old_norm} {done[total >= old_norm]}"
        else:
            count_line = f"Количество диалогов за день: {total}"

        txt = f"""Ник агента: @{nickname}
{count_line}
Бонусы за день: {bonuses} рублей{perenos_line}
Зарплата за день без учёта клиентов: {salary} рублей
{formula}
"""
        res.append(txt)

    report = '\n'.join(res) or f"Нет данных за {date_str}."
    if res:
        count = await req.count_day(date_str)
        report = f'Отчёт за {date_str}\n\n' + report + f'\nСуммарное количество сообщений за день: {count}'
    for i in range(0, len(report), 4096):
        await bot.send_message(chat_id=os.getenv('CHAT_ID'), text=report[i:i+4096])


async def week_res(bot):
    # Промежуточный отчёт за неделю
    report = await req.weekly_results()
    if report:
        for i in range(0, len(report), 4096):
            await bot.send_message(chat_id=os.getenv('CHAT_ID'), text=report[i:i+4096])

    # Итоговый отчёт за 2 недели
    report2 = await req.biweekly_results()
    if report2:
        for i in range(0, len(report2), 4096):
            await bot.send_message(chat_id=os.getenv('CHAT_ID'), text=report2[i:i+4096])
