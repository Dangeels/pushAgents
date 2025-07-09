import asyncio
import os

import pytz
import app.database.requests as req

import datetime

from aiogram.types import Message
from aiogram import F, Router
from aiogram.filters import CommandStart
from datetime import datetime, timedelta

router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message):
    print(message.chat.id)



async def day_res(bot):
    msk_tz = pytz.timezone("Europe/Moscow")
    now = datetime.now(msk_tz)
    date_str = now.strftime("%Y-%m-%d")
    dct = await req.daily_results(date_str)
    res = []
    done = {True: '(норма выполнена)', False: '(норма не выполнена)'}
    for key in dct.keys():
        if dct[key][1] < dct[key][4]:
            perenos = dct[key][1] % dct[key][4]
        else:
            perenos = dct[key][1] % dct[key][4] % 5

        txt = f"""Ник агента: @{dct[key][0]}
Количество диалогов за день: {dct[key][1]} {done[dct[key][1]>=dct[key][4]]}
Бонусы за день: {dct[key][2]} рублей
Перенос диалогов на завтра: {perenos} (завтрашняя норма {dct[key][5]})
Зарплата за день без учёта клиентов: {dct[key][3]} рублей
"""
        res.append(txt)

    for i in range(0, len(res), 4096):
        await bot.send_message(chat_id=os.getenv('CHAT_ID'), text='\n'.join(res)[i:i+4096])


async def week_res(bot):
    s = await req.weekly_results()
    for i in range(0, len(s), 4096):
        await bot.send_message(chat_id=os.getenv('CHAT_ID'), text=s[i:i+4096])


media_groups = set()


@router.message(F.photo)
async def dialogs_handler(message: Message):
    msk_tz = pytz.timezone("Europe/Moscow")
    now = datetime.now(msk_tz)
    date_str = now.strftime("%Y-%m-%d")

    await req.set_agent(message.from_user)
    await req.count_daily_messages(message.from_user, date_str, message)

    media = message.media_group_id
    if media:
        if media not in media_groups:
            media_groups.add(media)


    async def clean_processed_media_groups():
        await asyncio.sleep(10)
        media_groups.clear()

    asyncio.create_task(clean_processed_media_groups())
