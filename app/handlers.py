import asyncio
import os
import pytz
from datetime import datetime
from aiogram import Router, F
from aiogram.types import Message
from app.database import requests as req

router = Router()
media_groups = set()

@router.message(F.photo and F.caption)
async def dialogs_handler(message: Message):
    try:
        # Проверяем, что в подписи ровно одно упоминание (@)
        entities = message.caption_entities or []
        mention_entities = [e for e in entities if e.type in ("mention", "text_mention")]
        if len(mention_entities) != 1:
            await message.answer("Сообщение должно содержать ровно одно упоминание (@).")
            return

        msk_tz = pytz.timezone("Europe/Moscow")
        now = datetime.now(msk_tz)
        date_str = now.strftime("%Y-%m-%d")

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
    msk_tz = pytz.timezone("Europe/Moscow")
    now = datetime.now(msk_tz)
    date_str = now.strftime("%Y-%m-%d")
    dct = await req.daily_results(date_str)
    res = []
    done = {True: '(норма выполнена)', False: '(норма не выполнена)'}
    for key in dct.keys():
        if dct[key][1] < dct[key][4]:
            perenos = dct[key][1]
        else:
            perenos = (dct[key][1] - dct[key][4]) % 5

        txt = f"""Ник агента: @{dct[key][0]}
Количество диалогов за день: {dct[key][1]} {done[dct[key][1]>=dct[key][4]]}
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