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


@router.message(Command('all_admins') and F.chat.type == 'private')
async def all_admins(message: Message):
    if is_admin(message.from_user.username):
        admins = await req.all_admins()
        await message.answer('Список администраторов\n'+'\n'.join(admins[0]))


@router.message(Command('delete_admin') and F.chat.type == 'private')  # формат сообщения /delete_admin @user
async def delete_admin(message: Message):
    try:
        username = message.text.split()[1].strip('@')
        ad = await is_admin(message.from_user.username)
        ad2 = await is_admin(username)
        if ad[0] and ad[2] and ad2[0]:
            dct = {True: 'Пользователь успешно удалён из списка администраторов',
                   False: 'Ошибка доступа: вы не можете удалить этого пользователя'}
            c = await req.delete_admin(message.from_user, username)
            await message.answer(f'{dct[c]}')
        else:
            await message.answer('Пользователь не найден в списке администраторов')
    except Exception:
        await message.answer('Ошибка в формате сообщения')


@router.message(Command('set_admin') and F.chat.type == 'private')  # сообщение формата /set_admin @username
async def set_admin(message: Message):
    try:
        ad = await is_admin(message.from_user.username)
        if ad[0] and ad[2]:
            username = message.text.split()[1].strip('@')
            await req.set_admin(username)
            await message.answer(f'@{username} успешно добавлен в список администраторов')
    except Exception:
        await message.answer('Ошибка в формате сообщения')


async def is_admin(username):
    x = await req.is_admin(username)
    return x


def get_current_date():
    msk_tz = pytz.timezone("Europe/Moscow")
    now = datetime.now(msk_tz)
    date_str = now.strftime("%Y-%m-%d")
    return date_str


@router.message(Command('help') and F.chat.type == 'private')
async def help(message: Message):
    ad = await is_admin(message.from_user.username)
    if ad[0]:
        txt = """
        Команда /all_agents - получить список всех работающих агентов
        Команда /all_daily_messages - получить список всех диалогов за сегодня       
        
        Команда /reset_norm @username - сбросить/установить другую дневную норму у агента; 
            пример использования: /reset_norm @username 20 - установит агенту @username дневную норму в 20 диалогов
                                  /reset_norm @username - сбросит агенту @username дневную норму до базового значения (15)
                                  
        Команда /delete_dialog @agent_username @client_username - удалить один диалог агента из базы данных
            где @agent_username указывается никнейм агента, где @client_username указывается никнейм клиента из неправильного отчёта
            удалять можно только диалоги за текущий день
        
        Команда /all_admins - получить список всех администраторов 
        Команда /set_admin @username - добавить нового администратора
        Команда /delete_admin @username - удалить администратора
"""
        await message.answer(txt)


@router.message(Command('all_agents') and F.chat.type == 'private')
async def all_agents(message: Message):
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    agents = await req.all_agents()
    await message.answer('\n'.join(agents[0]))


@router.message(Command('all_daily_messages') and F.chat.type == 'private')
async def all_daily_messages(message: Message):
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    current_date = get_current_date()
    messages = await req.all_daily_messages(current_date)
    await message.answer(f'Сообщения за {current_date}\n'+'\n'.join(messages))


@router.message(Command('reset_norm') and F.chat.type == 'private')  # сообщение формата /reset_norm nickname 15
async def reset_norm(message: Message):
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
            norm = int(os.getenv('NORM'))
        agents = await req.all_agents()
        if username in agents[1]:
            await req.reset_norm(username, norm)
            await message.answer(f'Дневная норма @{username} успешно сброшена до {norm}')
        else:
            await message.answer(f'Агента @{username} не существует')
    except Exception:
        await message.answer('Неверный формат сообщения')


@router.message(Command('delete_dialog') and F.chat.type == 'private')  # сообщение формата /delete_dialog agent_nickname client_nickname
async def delete_dialog(message: Message):
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
        await message.answer('Неверный формат сообщения')


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
        await req.count_daily_messages(message.from_user, date_str, message)

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
    res = []
    done = {True: '(норма выполнена)', False: '(норма не выполнена)'}
    for key in dct.keys():
        if dct[key][1] < dct[key][4]:
            perenos = dct[key][1]
        else:
            perenos = (dct[key][1] - dct[key][4]) % 5

        txt = f"""Ник агента: @{dct[key][0]}
Количество диалогов за день: {dct[key][1]} {done[dct[key][1] >= dct[key][4]]}
Бонусы за день: {dct[key][2]} рублей
Перенос диалогов на завтра: {perenos} (завтрашняя норма {dct[key][5]})
Зарплата за день без учёта клиентов: {dct[key][3]} рублей
"""
        res.append(txt)

    report = '\n'.join(res) or f"Нет данных за {date_str}."
    for i in range(0, len(report), 4096):
        await bot.send_message(chat_id=os.getenv('CHAT_ID'), text=report[i:i+4096])


async def week_res(bot):
    report = await req.weekly_results()
    for i in range(0, len(report), 4096):
        await bot.send_message(chat_id=os.getenv('CHAT_ID'), text=report[i:i+4096])
