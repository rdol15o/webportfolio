# версия aiogram 3
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, Text
from bot_core.handlers.basic import start_msg, get_main_menu, get_accounts, get_money, get_deals, get_analytics
from bot_core.utils.commands import set_commands
import asyncio
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

TELEPORT_FOLIO_BOT_TOKEN = os.getenv('TELEPORT_FOLIO_BOT_TOKEN')
ADMIN_ID = os.getenv('ADMIN_ID')


async def start_msg_to_admin(bot: Bot):
    await set_commands(bot)
    await bot.send_message(chat_id=ADMIN_ID,
                           text='Бот запущен')


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
    dp.message.register(get_money, Text(text='деньги'))
    dp.message.register(get_deals, Text(text='сделки'))
    dp.message.register(get_deals, Text(text='>'))
    dp.message.register(get_analytics, Text(text='аналитика'))

    try:
        await dp.start_polling(bot, skip_updates=True)
    finally:
        await bot.session.close()


if __name__ == '__main__':
    asyncio.run(start_bot())
