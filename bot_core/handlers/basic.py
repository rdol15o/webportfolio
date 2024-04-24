from aiogram import Bot, types
from db_funcs import add_user_db, get_accounts_db, get_deals_db, get_cash_db, get_refills_db, \
    get_analytics_total_db, get_analytics_daily_db
from bot_core.keyboards.reply import get_main_keyboard, get_accounts_keyboard, get_cash_keyboard, get_analytics_keyboard, get_deals_keyboard


async def start_msg(message: types.Message, bot: Bot):
    add_user_db(message.from_user.id, message.from_user.first_name)
    await bot.send_message(chat_id=message.from_user.id,
                           text=f'Привет, {message.from_user.first_name}!\n'
                                f'Здесь ты можешь добавлять счета и сделки по бумагам,'
                                f' а также смотреть аналитику по портфелю.',
                           reply_markup=get_main_keyboard())


async def get_main_menu(message: types.Message, bot: Bot):
    await bot.send_message(chat_id=message.from_user.id,
                           text=f'Здесь ты можешь добавлять счета и сделки по бумагам,'
                                f' а также смотреть аналитику по портфелю.',
                           reply_markup=get_main_keyboard())


async def get_accounts(message: types.Message, bot: Bot):
    msg = 'Твои счета:\n'
    msg += get_accounts_db(tgm_user_id=message.from_user.id)
    await bot.send_message(chat_id=message.from_user.id,
                           text=msg,
                           reply_markup=get_accounts_keyboard())


async def get_cash(message: types.Message, bot: Bot):
    msg = 'Денежные средства:\n'
    msg += get_cash_db(tgm_user_id=message.from_user.id)
    msg += '\n\nВложенные средства:\n'
    msg += get_refills_db(tgm_user_id=message.from_user.id)
    await bot.send_message(chat_id=message.from_user.id,
                           text=msg,
                           reply_markup=get_cash_keyboard())


async def get_deals(message: types.Message, bot: Bot):
    msg = 'Последние сделки:\n'
    msg += '<pre>'
    msg += get_deals_db(tgm_user_id=message.from_user.id)
    msg += '</pre>'
    await bot.send_message(chat_id=message.from_user.id,
                           text=msg,
                           reply_markup=get_deals_keyboard())


async def get_analytics_menu(message: types.Message, bot: Bot):
    msg = 'Выберите отчет\n'
    await bot.send_message(chat_id=message.from_user.id,
                           text=msg,
                           reply_markup=get_analytics_keyboard())


async def get_analytics_total_simple(message: types.Message, bot: Bot):
    msg = 'Изменение бумаг:\n'
    msg += '<pre>'
    msg += get_analytics_total_db(tgm_user_id=message.from_user.id, is_simple=True)
    msg += '</pre>'
    await bot.send_message(chat_id=message.from_user.id,
                           text=msg,
                           reply_markup=get_main_keyboard())


async def get_analytics_total(message: types.Message, bot: Bot):
    msg = 'Изменение бумаг:\n'
    msg += '<pre>'
    msg += get_analytics_total_db(tgm_user_id=message.from_user.id, is_simple=False)
    msg += '</pre>'
    await bot.send_message(chat_id=message.from_user.id,
                           text=msg,
                           reply_markup=get_main_keyboard())


async def get_analytics_daily_simple(message: types.Message, bot: Bot):
    msg = 'Изменение бумаг за день:\n'
    msg += '<pre>'
    msg += get_analytics_daily_db(tgm_user_id=message.from_user.id, is_simple=True)
    msg += '</pre>'
    await bot.send_message(chat_id=message.from_user.id,
                           text=msg,
                           reply_markup=get_main_keyboard())