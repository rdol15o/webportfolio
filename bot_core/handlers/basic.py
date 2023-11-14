from aiogram import Bot, types
from db_funcs import add_user_db, get_accounts_db, get_deals_db
from bot_core.keyboards.reply import get_main_keyboard, get_accounts_keyboard, get_money_keyboard, get_analytics_keyboard, get_deals_keyboard


async def start_msg(message: types.Message, bot: Bot):
    add_user_db(message.from_user.id, message.from_user.first_name)
    await bot.send_message(chat_id=message.from_user.id,
                           text=f'Привет, {message.from_user.first_name}!\n'
                                f'Здесь ты можешь добавлять счета и сделки по бумагам,'
                                f' а также смотреть аналитику.',
                           reply_markup=get_main_keyboard())


async def get_main_menu(message: types.Message, bot: Bot):
    await bot.send_message(chat_id=message.from_user.id,
                           text=f'Здесь ты можешь добавлять счета и сделки по бумагам,'
                                f' а также смотреть аналитику.',
                           reply_markup=get_main_keyboard())


async def get_accounts(message: types.Message, bot: Bot):
    msg = 'Твои счета:\n'
    msg += get_accounts_db(message.from_user.id)
    await bot.send_message(chat_id=message.from_user.id,
                           text=msg,
                           reply_markup=get_accounts_keyboard())


async def get_money(message: types.Message, bot: Bot):
    msg = 'Денежные средства:\n'
    await bot.send_message(chat_id=message.from_user.id,
                           text=msg,
                           reply_markup=get_money_keyboard())


async def get_deals(message: types.Message, bot: Bot):
    msg = 'Последние сделки:\n'
    # msg += '<pre>'
    msg += get_deals_db(message.from_user.id)
    # msg += '</pre>'
    await bot.send_message(chat_id=message.from_user.id,
                           text=msg,
                           reply_markup=get_deals_keyboard())


async def get_analytics(message: types.Message, bot: Bot):
    await bot.send_message(chat_id=message.from_user.id,
                           text='Выбери желаемый анализ:',
                           reply_markup=get_analytics_keyboard())
