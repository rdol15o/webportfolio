# версия aiogram 3
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, Text
from bot_core.handlers.basic import start_msg, get_main_menu, get_accounts, get_cash, get_deals, get_analytics_menu, get_analytics_total_simple, get_analytics_total
from bot_core.utils.commands import set_commands
from bot_core.keyboards.reply import get_main_keyboard
import asyncio
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

TELEPORT_FOLIO_BOT_TOKEN = os.getenv('TELEPORT_FOLIO_BOT_TOKEN')
ADMIN_ID = os.getenv('ADMIN_ID')


async def start_msg_to_admin(bot: Bot):
    await set_commands(bot)
    await bot.send_message(chat_id=ADMIN_ID,
                           text='Бот запущен',
                           reply_markup=get_main_keyboard())


async def finish_msg_to_admin(bot: Bot):
    await bot.send_message(chat_id=ADMIN_ID,
                           text='Бот остановлен')


async def start_bot():
    logging.basicConfig(level=logging.INFO)
    bot = Bot(token=TELEPORT_FOLIO_BOT_TOKEN, parse_mode='HTML')
    dp = Dispatcher()
    dp.startup.register(start_msg_to_admin)
    dp.shutdown.register(finish_msg_to_admin)
    dp.message.register(start_msg, Command(commands='start'))
    dp.message.register(get_main_menu, Text(text='главное меню'))
    dp.message.register(get_accounts, Text(text='счета'))
    dp.message.register(get_cash, Text(text='деньги'))
    dp.message.register(get_deals, Text(text='сделки'))
    dp.message.register(get_deals, Text(text='>'))
    dp.message.register(get_analytics_menu, Text(text='аналитика'))
    dp.message.register(get_analytics_total_simple, Text(text='бумаги за весь период'))
    dp.message.register(get_analytics_total, Text(text='бумаги за весь период (подробно)'))

    try:
        await dp.start_polling(bot, skip_updates=True)
    finally:
        await bot.session.close()


if __name__ == '__main__':
    asyncio.run(start_bot())
