from aiogram import types


def menu():
    keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_menu = ["Начать парсинг", 'Настройки']
    buttons_menu1 = ['Тех поддержка']
    buttons_menu2 = ['Активировать подписку', "Пополнить баланс"]
    keyboard_menu.add(*buttons_menu)
    keyboard_menu.add(*buttons_menu1)
    keyboard_menu.add(*buttons_menu2)
    return keyboard_menu


def admin():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text="Изменить промокоды", callback_data="update_promo"))
    keyboard.add(types.InlineKeyboardButton(text="Увеличить баланс", callback_data="stonks"))
    keyboard.add(types.InlineKeyboardButton(text="Изменить Прокси", callback_data="add_proxy"))
    return keyboard


def pay():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text="Ввести промокод", callback_data="set_promo"))
    keyboard.add(types.InlineKeyboardButton(text="Пополнить с помощью Qiwi", callback_data="qiwi"))
    return keyboard


def back():
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons = ["Назад"]
    keyboard.add(*buttons)
    return keyboard


def sub():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text="1 день - 150 RUB", callback_data="sub_1"))
    keyboard.add(types.InlineKeyboardButton(text="3 дня - 350 RUB", callback_data="sub_2"))
    keyboard.add(types.InlineKeyboardButton(text="7 дней - 700 RUB", callback_data="sub_3"))
    keyboard.add(types.InlineKeyboardButton(text="30 дней - 2500 RUB", callback_data="sub_4"))
    return keyboard


def servis():
    keyboard = types.InlineKeyboardMarkup()
    # keyboard.add(types.InlineKeyboardButton(text="Ebay 🇩🇪", callback_data="Ebay"))
    keyboard.add(types.InlineKeyboardButton(text="Bolha 🇸🇮", callback_data="Bolha"))
    keyboard.add(types.InlineKeyboardButton(text="Gumtree 🇬🇧", callback_data="Gumtree"))
    keyboard.add(types.InlineKeyboardButton(text="OLX 🇵🇱", callback_data="OLX"))
    # keyboard.add(types.InlineKeyboardButton(text="OLX 🇺🇦", callback_data="OLXua"))
    keyboard.add(types.InlineKeyboardButton(text="Wallapop 🇬🇧 🇮🇹", callback_data="wallapop"))
    # keyboard.add(types.InlineKeyboardButton(text="Tutti CH", callback_data="tutti"))
    return keyboard


def wats(phone, url_text, item):
    buttons = [
        types.InlineKeyboardButton(text='Написать в whatsap',
                                   url='https://wa.me/{0}?text={1}{2}'.format(phone, url_text,
                                                                              item))
    ]
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(*buttons)
    return keyboard


def stop():
    keyboard_stop = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_stop = ["Остановить парсинг"]
    keyboard_stop.add(*buttons_stop)
    return keyboard_stop
