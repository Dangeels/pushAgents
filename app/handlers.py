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
        txt = (
            "Помощь и команды\n\n"
            "Отчёты\n"
            "- Ежедневный: итог по каждому агенту/группе за день.\n"
            "- Недельный: <ПРОМЕЖУТОЧНЫЙ НЕДЕЛЬНЫЙ ОТЧЁТ ПО ДИАЛОГАМ> — для мониторинга за неделю.\n"
            "- Двухнедельный: <ИТОГОВЫЙ ОТЧЁТ ЗА 2 НЕДЕЛИ> — учитывает премию за наибольшее число диалогов.\n\n"

            "Работа с диалогами\n"
            "- /add_dialog @agent @client — добавить 1 диалог агенту вручную (если бот не распознал отчёт).\n"
            "- /delete_dialog @agent @client — удалить 1 ошибочно засчитанный диалог за текущий день.\n"
            "- /all_daily_messages — показать все диалоги за сегодня.\n"
            "- /all_time_messages — общее число диалогов за всё время.\n\n"

            "Отправка отчётов агентами\n"
            "- Агент присылает фото/видео с подписью, в которой ТОЛЬКО один @username клиента.\n"
            "- Если в подписи есть что-то кроме @username или упоминаний больше одного — отчёт не засчитывается.\n\n"

            "Группы агентов (объединение нескольких никнеймов в одного «агента» для отчётов)\n"
            "- /set_group Название @nick1 @nick2 ... — создать/обновить группу. В отчётах ники суммируются как один агент с указанным Названием.\n"
            "  Пример: /set_group Агент_Кирилл @nickname1 @nickname2\n"
            "- /list_groups — список групп и их участников.\n\n"

            "Выплаты и нормы\n"
            "- По умолчанию нормы выключены. Оплата: 1 диалог = X рублей (X=20 по умолчанию).\n"
            "- /set_rate X — установить базовую ставку за 1 диалог (когда нормы выключены).\n"
            "- /enable_norms on|off — включить/выключить режим норм глобально.\n"
            "- /set_global_norm X — ставка по умолчанию при включённых нормах.\n"
            "- /set_agent_norm @nick X — задать индивидуальную ставку (норму) для конкретного агента.\n\n"

            "Корректировки (не удаляют клиента из базы)\n"
            "- /adjust_dialogs @nick DELTA [YYYY-MM-DD] [причина] — добавить/вычесть диалоги агенту за конкретный день.\n"
            "  Делайте DELTA отрицательной, чтобы вычесть (пример: -3).\n"
            "  Пример: /adjust_dialogs @nick -2 2025-11-03 Некорректный отчёт\n"
            "- /get_settings — текущие настройки ставок, норм и премии.\n\n"

            "Администрирование\n"
            "- /all_admins — список администраторов.\n"
            "- /set_admin @user — добавить администратора.\n"
            "- /delete_admin @user — удалить администратора.\n"
        )
        await message.answer(txt)


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
    # Функция отключена в новой модели без норм и бонусов
    await message.answer('Команда отключена: нормы и бонусы больше не используются')


@router.message(Command('reset_norm'))  # сообщение формата /reset_norm nickname 15
async def reset_norm(message: Message):
    if message.chat.type != 'private':
        return
    # Функция отключена в новой модели без норм и бонусов
    await message.answer('Команда отключена: нормы и бонусы больше не используются')


# Настройки выплат/норм
@router.message(Command('set_rate'))
async def set_rate(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not (ad[0] and ad[2]):
        await message.answer('Доступ запрещён')
        return
    try:
        rate = int(message.text.split()[1])
        await req.set_rate_per_dialog(rate)
        await message.answer(f'Ставка за диалог обновлена: {rate} руб')
    except Exception:
        await message.answer('Формат: /set_rate X')


@router.message(Command('enable_norms'))
async def enable_norms(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not (ad[0] and ad[2]):
        await message.answer('Доступ запрещён')
        return
    try:
        flag = message.text.split()[1].lower()
        val = flag in ('on', '1', 'true', 'yes')
        await req.set_norms_enabled(val)
        await message.answer(f'Нормы глобально: {"включены" if val else "выключены"}')
    except Exception:
        await message.answer('Формат: /enable_norms on|off')


@router.message(Command('set_global_norm'))
async def set_global_norm(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not (ad[0] and ad[2]):
        await message.answer('Доступ запрещён')
        return
    try:
        rate = int(message.text.split()[1])
        await req.set_global_norm_rate(rate)
        await message.answer(f'Глобальная ставка при нормах: {rate} руб')
    except Exception:
        await message.answer('Формат: /set_global_norm X')


@router.message(Command('set_agent_norm'))
async def set_agent_norm(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not (ad[0] and ad[2]):
        await message.answer('Доступ запрещён')
        return
    try:
        parts = message.text.split()
        nickname = parts[1].lstrip('@')
        rate = int(parts[2])
        ok = await req.set_agent_norm(nickname, rate)
        await message.answer('ОК' if ok else 'Агент не найден')
    except Exception:
        await message.answer('Формат: /set_agent_norm @nick X')


@router.message(Command('get_settings'))
async def get_settings(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    st = await req.get_settings()
    text = (
        f"Ставка за диалог: {st.rate_per_dialog}\n"
        f"Нормы: {'включены' if st.norms_enabled else 'выключены'}\n"
        f"Глобальная ставка при нормах: {st.global_norm_rate}\n"
        f"Премия за наибольшее количество диалогов: {getattr(st, 'top_bonus', 300)}"
    )
    await message.answer(text)


@router.message(Command('adjust_dialogs'))
async def adjust_dialogs(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not (ad[0] and ad[2]):
        await message.answer('Доступ запрещён')
        return
    try:
        parts = message.text.split()
        if len(parts) < 3:
            await message.answer('Формат: /adjust_dialogs @nick DELTA [YYYY-MM-DD] [причина]')
            return
        nickname = parts[1].lstrip('@')
        delta = int(parts[2])
        # Опциональная дата
        date_str = parts[3] if len(parts) >= 4 and '-' in parts[3] else get_current_date()
        # Причина
        reason_start = 4 if len(parts) >= 4 and '-' in parts[3] else 3
        reason = ' '.join(parts[reason_start:]) if len(parts) > reason_start else ''
        ok = await req.add_adjustment(nickname, delta, date_str, reason)
        await message.answer('Корректировка сохранена' if ok else 'Агент не найден')
    except Exception:
        await message.answer('Формат: /adjust_dialogs @nick DELTA [YYYY-MM-DD] [причина]')


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


# Новые команды управления группами
@router.message(Command('set_group'))
async def set_group(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not (ad[0] and ad[2]):
        await message.answer('Доступ запрещён')
        return
    try:
        parts = message.text.split()
        if len(parts) < 3:
            await message.answer('Формат: /set_group Название @nick1 @nick2 ...')
            return
        title = parts[1]
        members = [p.lstrip('@') for p in parts[2:]]
        await req.ensure_group(title)
        for nick in members:
            await req.add_member_to_group(title, nick)
        await message.answer(f'Группа "{title}" обновлена: ' + ', '.join([f'@{m}' for m in members]))
    except Exception as e:
        await message.answer(f'Ошибка: {e}')


@router.message(Command('list_groups'))
async def list_groups(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    data = await req.list_groups()
    if not data:
        await message.answer('Группы не заданы')
        return
    lines = []
    for title, members in data.items():
        members_txt = ', '.join([f'@{m}' for m in members]) or '—'
        lines.append(f'{title}: {members_txt}')
    await message.answer('\n'.join(lines))


async def day_res(bot):
    date_str = get_current_date()
    dct = await req.daily_results(date_str)
    res = []
    for key in dct.keys():
        if dct[key][1] == 0:
            continue
        txt = f"""Агент: {dct[key][0]}
Количество диалогов за день: {dct[key][1]}
Зарплата за день: {dct[key][2]} рублей
"""
        res.append(txt)

    report = '\n'.join(res) or f"Нет данных за {date_str}."
    if res:
        count = await req.count_day(date_str)
        report = f'Отчёт за {date_str}\n\n' + report + f'\nСуммарное количество сообщений за день: {count}'
    for i in range(0, len(report), 4096):
        await bot.send_message(chat_id=os.getenv('CHAT_ID'), text=report[i:i+4096])


async def week_res(bot):
    report = await req.weekly_results()
    if not report:
        return
    for i in range(0, len(report), 4096):
        await bot.send_message(chat_id=os.getenv('CHAT_ID'), text=report[i:i+4096], parse_mode='HTML')


async def biweek_res(bot):
    report = await req.biweekly_results()
    if not report:
        return
    for i in range(0, len(report), 4096):
        await bot.send_message(chat_id=os.getenv('CHAT_ID'), text=report[i:i+4096], parse_mode='HTML')
