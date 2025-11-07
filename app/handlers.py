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


# ===== УТИЛИТЫ ПАРСИНГА ИДЕНТИФИКАТОРА АГЕНТА =====

def _strip_quotes(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and ((s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'")):
        return s[1:-1].strip()
    return s


def _after_command(text: str) -> str:
    return text.split(' ', 1)[1] if ' ' in text else ''


def _parse_first_arg_and_rest(text: str) -> tuple[str | None, str]:
    rest = _after_command(text).lstrip()
    if not rest:
        return None, ''
    if rest[0] in ('"', "'"):
        q = rest[0]
        end = rest.find(q, 1)
        if end == -1:
            return None, ''
        ident = rest[1:end]
        remainder = rest[end + 1:].strip()
        return ident.strip(), remainder
    parts = rest.split(maxsplit=1)
    ident = parts[0]
    remainder = parts[1] if len(parts) > 1 else ''
    return ident.strip(), remainder


def _parse_agent_only(text: str) -> str | None:
    ident, rem = _parse_first_arg_and_rest(text)
    return _strip_quotes(ident) if ident else None


def _parse_agent_and_last_token(text: str) -> tuple[str | None, str | None]:
    rest = _after_command(text)
    parts = rest.split()
    if len(parts) < 2:
        return None, None
    last = parts[-1]
    middle = rest[:rest.rfind(last)].strip()
    return _strip_quotes(middle), last


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

<i>Во всех командах, где раньше указывался @username агента, теперь можно указывать либо @username, либо "Название агента" (в кавычках, если есть пробелы).</i>

<b>1) Диалоги и корректировки</b>
• /add_dialog &lt;агент&gt; @client — добавить 1 диалог агенту за текущий день.
  Пример: /add_dialog @ivan @client123 или /add_dialog "Иван Иванов" @client123.
• /delete_dialog &lt;агент&gt; @client — удалить 1 диалог за текущий день у агента и удалить клиента из базы clients.
• /sub_dialogs &lt;агент&gt; &lt;N&gt; — вычесть N диалогов у агента за сегодня без удаления клиентов.
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
• /agent_norms &lt;агент&gt; on|off — включить/выключить нормы для агента.
• /norms_global on|off — глобально включить или выключить нормы (только старшие админы).
• /set_dialog_price &lt;стоимость&gt; — цена одного диалога, если нормы отключены.
• /set_top_premium &lt;сумма&gt; — премия топ-агенту за 2 недели.

<b>3) Агенты и аккаунты</b>
• /all_agents — показать всех агентов, их аккаунты и текущую норму.
• /link_account &lt;агент&gt; @account — привязать аккаунт (по username) к агенту.
• /unlink_account &lt;агент&gt; @account — отвязать аккаунт от агента.
• /set_primary &lt;агент&gt; @account — задать primary-аккаунт агента.
• /list_accounts &lt;агент&gt; — показать все аккаунты агента (primary помечен).
• /all_accounts — показать все аккаунты всех агентов (primary помечен).
• /delete_agent &lt;агент&gt; — полностью удалить агента и его аккаунты.
• /set_agent_name &lt;агент&gt; &lt;Новое название&gt; — задать человекочитаемое имя для агента.

<b>4) Администрирование</b>
• /all_admins — показать список администраторов.
• /set_admin @username — добавить администратора.
• /delete_admin @username — удалить администратора (уровень прав учитывается).
"""
        await message.answer(txt)


@router.message(Command('agent_norms'))  # /agent_norms <agent> on|off
async def agent_norms(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        ident, state = _parse_first_arg_and_rest(message.text)
        if not ident or not state:
            raise ValueError
        state = state.lower().strip()
        enabled = state in ['on', '1', 'true', 'вкл', 'enable']
        ok = await req.set_agent_norms(_strip_quotes(ident), enabled)
        if ok:
            await message.answer(f"Нормы для агента '{_strip_quotes(ident)}' {'включены' if enabled else 'выключены'}")
        else:
            await message.answer(f"Агент '{_strip_quotes(ident)}' не найден")
    except Exception:
        await message.answer('Неверный формат. Пример: /agent_norms "Иван Иванов" on')


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


@router.message(Command('reset_norm'))  # /reset_norm <agent> [norm]
async def reset_norm(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        rest = _after_command(message.text)
        if not rest:
            raise ValueError
        parts = rest.split()
        # если последний токен — число, это норма
        norm_val = None
        try:
            norm_val = int(parts[-1])
            agent_ident = rest[:rest.rfind(parts[-1])].strip()
        except ValueError:
            agent_ident = rest.strip()
            norm_obj = await req.get_norm()
            norm_val = norm_obj.norm
        agent_ident = _strip_quotes(agent_ident)
        ok = await req.reset_norm(agent_ident, abs(int(norm_val)))
        if ok:
            await message.answer(f"Дневная норма агента '{agent_ident}' успешно сброшена до {abs(int(norm_val))}")
        else:
            await message.answer(f"Агент '{agent_ident}' не существует")
    except Exception:
        await message.answer('Неверный формат. Пример: /reset_norm "Иван Иванов" 15')


@router.message(Command('link_account'))  # /link_account <agent> @account
async def link_account(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        agent_ident, account_username = _parse_agent_and_last_token(message.text)
        if not agent_ident or not account_username or not account_username.startswith('@'):
            raise ValueError
        ok, text = await req.link_account(agent_ident, account_username.strip())
        await message.answer(text)
    except Exception:
        await message.answer('Неверный формат. Пример: /link_account "Иван Иванов" @account')


@router.message(Command('unlink_account'))  # /unlink_account <agent> @account
async def unlink_account(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        agent_ident, account_username = _parse_agent_and_last_token(message.text)
        if not agent_ident or not account_username or not account_username.startswith('@'):
            raise ValueError
        ok, text = await req.unlink_account(agent_ident, account_username.strip())
        await message.answer(text)
    except Exception:
        await message.answer('Неверный формат. Пример: /unlink_account "Иван Иванов" @account')


@router.message(Command('set_primary'))  # /set_primary <agent> @account
async def set_primary(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        agent_ident, account_username = _parse_agent_and_last_token(message.text)
        if not agent_ident or not account_username or not account_username.startswith('@'):
            raise ValueError
        ok, text = await req.set_primary_account(agent_ident, account_username.strip())
        await message.answer(text)
    except Exception:
        await message.answer('Неверный формат. Пример: /set_primary "Иван Иванов" @account')


# Новый обработчик: показать привязанные аккаунты агента
@router.message(Command('list_accounts'))  # /list_accounts <agent>
async def list_accounts(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        agent_ident = _parse_agent_only(message.text)
        if not agent_ident:
            raise ValueError
        accounts = await req.list_accounts(agent_ident)
        if not accounts:
            await message.answer(f"Агент '{agent_ident}' не найден или у него нет привязанных аккаунтов")
            return
        await message.answer('Аккаунты агента ' + agent_ident + ' (primary помечен):\n- ' + '\n- '.join(accounts))
    except Exception:
        await message.answer('Неверный формат. Пример: /list_accounts "Иван Иванов"')


@router.message(Command('delete_agent'))  # /delete_agent <agent>
async def delete_agent(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        agent_ident = _parse_agent_only(message.text)
        if not agent_ident:
            raise ValueError
        ok, text = await req.delete_agent(agent_ident)
        await message.answer(text)
    except Exception:
        await message.answer('Неверный формат. Пример: /delete_agent "Иван Иванов"')


@router.message(Command('sub_dialogs'))  # /sub_dialogs <agent> N
async def sub_dialogs(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        rest = _after_command(message.text)
        if not rest:
            raise ValueError
        parts = rest.split()
        amount = int(parts[-1])
        if amount <= 0:
            await message.answer('Число должно быть положительным')
            return
        agent_ident = _strip_quotes(rest[:rest.rfind(parts[-1])].strip())
        date_str = get_current_date()
        ok, text, _sub = await req.subtract_dialogs(agent_ident, date_str, amount)
        # Пересчитаем зарплату за текущий день после корректировки
        await req.daily_results(date_str)
        await message.answer(text)
    except ValueError:
        await message.answer('Неверный формат. Пример: /sub_dialogs "Иван Иванов" 3')
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


@router.message(Command('add_dialog'))  # /add_dialog <agent> @client
async def add_dialog(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        agent_ident, client_nickname = _parse_agent_and_last_token(message.text)
        if not agent_ident or not client_nickname or not client_nickname.startswith('@'):
            raise ValueError
        status, nick = await req.add_dialog(agent_ident, client_nickname.strip('@'), get_current_date())
        if status == 'not_agent':
            await message.answer('Этого агента нет в базе данных. Сначала ему требуется отправить хотя бы один отчёт')
        elif status == 'повтор':
            await message.answer('Повтор. Этот клиент ранее упоминался в отчёте')
        elif status == 'ok':
            await message.answer(f'Диалог успешно добавлен агенту @{nick}')
        else:
            await message.answer('Ошибка добавления диалога')
    except Exception:
        await message.answer('Неверный формат. Пример: /add_dialog "Иван Иванов" @client_nickname')


@router.message(Command('delete_dialog'))  # /delete_dialog <agent> @client
async def delete_dialog(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        agent_ident, client_nickname = _parse_agent_and_last_token(message.text)
        if not agent_ident or not client_nickname or not client_nickname.startswith('@'):
            raise ValueError
        date_str = get_current_date()
        status, nick = await req.delete_dialog(agent_ident, client_nickname.strip('@'), date_str)
        if status == 'not_agent':
            await message.answer('Неверный агент — не найден')
            return
        ms = await req.all_daily_messages(date_str)
        m = [i for i in ms if f'(@{nick})' in i]
        await message.answer(f'Сообщение успешно удалено\nСообщения агента за {date_str}\n' + ''.join(m))
    except Exception:
        await message.answer('Неверный формат. Пример: /delete_dialog "Иван Иванов" @client_nickname')


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
            _bg_task = asyncio.create_task(clean_processed_media_groups(media))
            # предотвращаем предупреждения линтера: задача запущена фоново
            _bg_task.add_done_callback(lambda t: t.exception())

    except Exception as e:
        print(f"Ошибка в обработке сообщения: {e}")
        await message.answer("Произошла ошибка при обработке сообщения.")


async def clean_processed_media_groups(media_group_id):
    await asyncio.sleep(10)
    media_groups.discard(media_group_id)


@router.message(Command('set_agent_name'))  # /set_agent_name <agent> <New display name>
async def set_agent_name(message: Message):
    if message.chat.type != 'private':
        return
    ad = await is_admin(message.from_user.username)
    if not ad[0]:
        await message.answer('Доступ запрещён')
        return
    try:
        ident, remainder = _parse_first_arg_and_rest(message.text)
        if not ident or not remainder:
            raise ValueError
        agent_ident = _strip_quotes(ident)
        new_name = remainder.strip()
        ok, text = await req.set_agent_display_name(agent_ident, new_name)
        await message.answer(text)
    except Exception:
        await message.answer('Неверный формат. Пример: /set_agent_name "Иван Иванов" Новое Название')


async def day_res(bot):
    date_str = get_current_date()
    dct = await req.daily_results(date_str)
    # Получаем глобальные настройки норм (зарплата/бонусы/включены ли нормы)
    norm_cfg = await req.get_norm()
    res = []
    done = {True: '(норма выполнена)', False: '(норма не выполнена)'}
    for key in dct.keys():
        # dct[key] = [nickname, display_name, total, bonuses, salary, old_norm, new_norm]
        nickname = dct[key][0]
        display_name = dct[key][1] or f'@{nickname}'
        total = dct[key][2]
        bonuses = dct[key][3]
        salary = dct[key][4]
        old_norm = dct[key][5]
        new_norm = dct[key][6]
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

        # Строка количества диалогов
        if norms_active:
            count_line = f"Количество диалогов за день: {total}/{old_norm} {done[total >= old_norm]}"
        else:
            count_line = f"Количество диалогов за день: {total}"

        txt = f"""Агент: {display_name} (@{nickname})
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
