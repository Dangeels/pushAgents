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
    if is_admin(message.from_user.username):
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
        Команда /all_agents - получить список всех работающих агентов
Команда /all_daily_messages - получить список всех диалогов за сегодня       
Команда /all_time_messages - получить количество диалогов за всё время        
        
Команда /set_new_norm 20 - установить новую дневную норму для всех
После команды через пробел указывается число - новая норма
Доступ к команде есть только у старших админов
        
Команда /reset_norm @username - сбросить/установить другую дневную норму у агента; 
Пример использования: 
● /reset_norm @username 20 - установит агенту @username дневную норму в 20 диалогов
● /reset_norm @username - сбросит агенту @username дневную норму до базового значения (15)


Команда /add_dialog @agent @client - добавить один диалог агенту, 
● где @agent указывается никнейм агента, 
● где @client указывается никнейм клиента из неправильного отчёта
● отправляется одним сообщением, прикреплять фото/видео к сообщению не требуется
                                  
Команда /delete_dialog @agent @client - удалить один диалог агента из базы данных, 
● где @agent указывается никнейм агента, 
● где @client указывается никнейм клиента из неправильного отчёта
● удалять можно только диалоги за текущий день

Команда /set_client @username - добавить обработанного клиента в базу данных
 
Команда /all_admins - получить список всех администраторов 
Команда /set_admin @username - добавить нового администратора
Команда /delete_admin @username - удалить администратора
"""
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


@router.message(Command('set_new_norm'))  # /set_new_norm [norm] [salary] [bonuses]
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
        await message.answer('Неверный формат сообщения')


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
    res = []
    done = {True: '(норма выполнена)', False: '(норма не выполнена)'}
    for key in dct.keys():
        if dct[key][1] == 0:
            continue
        if dct[key][1] < dct[key][4]:
            perenos = dct[key][1]
        else:
            perenos = (dct[key][1] - dct[key][4]) % 5

        txt = f"""Ник агента: @{dct[key][0]}
Количество диалогов за день: {dct[key][1]}/{dct[key][4]} {done[dct[key][1] >= dct[key][4]]}
Бонусы за день: {dct[key][2]} рублей
Перенос диалогов на завтра: {perenos} (завтрашняя норма {dct[key][5]})
Зарплата за день без учёта клиентов: {dct[key][3]} рублей
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
    for i in range(0, len(report), 4096):
        await bot.send_message(chat_id=os.getenv('CHAT_ID'), text=report[i:i+4096])
