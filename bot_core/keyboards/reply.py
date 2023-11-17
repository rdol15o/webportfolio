from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, KeyboardButtonPollType
from aiogram.utils.keyboard import ReplyKeyboardBuilder


def get_main_keyboard():
    keyboard_builder = ReplyKeyboardBuilder()

    keyboard_builder.button(text='счета')
    keyboard_builder.button(text='деньги')
    keyboard_builder.button(text='сделки')
    keyboard_builder.button(text='аналитика')

    # расположение кнопок
    keyboard_builder.adjust(3, 1)

    return keyboard_builder.as_markup(resize_keyboard=True)


def get_accounts_keyboard():
    keyboard_builder = ReplyKeyboardBuilder()

    keyboard_builder.button(text='удалить')
    keyboard_builder.button(text='добавить')
    keyboard_builder.button(text='главное меню')

    # расположение кнопок
    keyboard_builder.adjust(2, 1)

    return keyboard_builder.as_markup(resize_keyboard=True)


def get_cash_keyboard():
    keyboard_builder = ReplyKeyboardBuilder()

    keyboard_builder.button(text='удалить')
    keyboard_builder.button(text='добавить')
    keyboard_builder.button(text='главное меню')

    # расположение кнопок
    keyboard_builder.adjust(2, 1)

    return keyboard_builder.as_markup(resize_keyboard=True)


def get_deals_keyboard():
    keyboard_builder = ReplyKeyboardBuilder()

    keyboard_builder.button(text='|<<')
    keyboard_builder.button(text='<')
    keyboard_builder.button(text='>')
    keyboard_builder.button(text='>>|')
    keyboard_builder.button(text='удалить')
    keyboard_builder.button(text='добавить')
    keyboard_builder.button(text='главное меню')

    # расположение кнопок
    keyboard_builder.adjust(4, 2, 1)

    return keyboard_builder.as_markup(resize_keyboard=True)


def get_analytics_keyboard():
    keyboard_builder = ReplyKeyboardBuilder()

    keyboard_builder.button(text='бумаги за весь период')
    keyboard_builder.button(text='бумаги за весь период (подробно)')
    keyboard_builder.button(text='главное меню')

    # расположение кнопок
    keyboard_builder.adjust(2, 1)

    return keyboard_builder.as_markup(resize_keyboard=True)