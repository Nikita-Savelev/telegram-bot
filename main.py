from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram import Bot, Dispatcher, types, executor
from aiogram.dispatcher.filters import Text
from aiogram.dispatcher import FSMContext
import aiogram.utils.markdown as md

import psycopg2
import config
import keyboard
import ebay
import bolha
import gumtree
import olx
import olx_ua
import wallapop_uk
import wallapop_it
import tutti_ch

import requests

from pyqiwip2p import QiwiP2P
import pay

import re

import random

from datetime import datetime, timedelta

import urllib

p2p = QiwiP2P(
    auth_key=config.qiwi_auth_key)
bot = Bot(config.token_bot)
dp = Dispatcher(bot, storage=MemoryStorage())

# conn_m=sqlite3.connect('bd_bot.db', check_same_thread=False)
# cur_m=conn_m.cursor()

conn_m = psycopg2.connect(
    host=config.psycopg2_host,
    user=config.psycopg2_user,
    password=config.psycopg2_password,
    database=config.psycopg2_database,
    port=config.psycopg2_port
)
cur_m = conn_m.cursor()

HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'
}
count = 0


@dp.message_handler(Text(equals="Назад"))
async def menu(message: types.Message):
    money = pay.user_money(cur_m, conn_m, message.from_user.id)
    data = pay.check_time_for_pay(cur_m, conn_m, message.from_user.id)
    data = data[:16]
    await message.answer(f'баланс - {money} RUB\nдействие подписки закончится {data}', reply_markup=keyboard.menu())


@dp.message_handler(commands="sizam_open")
async def admin_panel(message: types.Message):
    await message.answer('пароль?')
    await set_password.password.set()


class set_password(StatesGroup):
    password = State()


@dp.message_handler(state=set_password.password)
async def get_password(message, state: FSMContext):
    if message.text == config.pw_admin:
        await message.answer('приветствую, создатель!\nчего желаешь?', reply_markup=keyboard.admin())
    async with state.proxy() as data:
        data['promo'] = message.text
    await state.finish()


@dp.callback_query_handler(text="add_proxy")
async def chech_proxy(call: types.CallbackQuery):
    cur_m.execute('SELECT proxy_list FROM proxy WHERE id = %s', ('123123',))
    try:
        proxy_list = cur_m.fetchall()[0][0]
    except:
        proxy_list = 'None'
    await call.message.answer(proxy_list)
    await proxx.prox.set()


class proxx(StatesGroup):
    prox = State()


@dp.message_handler(state=proxx.prox)
async def add_proxy(message, state: FSMContext):
    cur_m.execute('UPDATE proxy SET proxy_list = %s WHERE id = %s', (message.text, '123123'))
    conn_m.commit()
    await message.answer('прокси обновленныы повелитель!')
    await state.finish()


class stonks_(StatesGroup):
    stonks_ = State()


@dp.callback_query_handler(text="stonks")
async def stonks(call: types.CallbackQuery):
    await call.message.answer('введи id и сумму пополнения')
    await stonks_.stonks_.set()


@dp.message_handler(state=stonks_.stonks_)
async def stonks_money(message, state: FSMContext):
    id_money = message.text
    id_ = re.sub('\s.+', '', id_money)
    print(id_)
    money = re.sub('.+\s', '', id_money)
    print(money)
    if re.fullmatch('[0-9]+\s[0-9]+', id_money):
        pay.pay_load(conn_m, cur_m, id_, money)
        await message.answer('баланс пополнен, повелитель!')
        async with state.proxy() as data:
            data['stonks_'] = message.text
        await state.finish()
    else:
        await message.answer('дурак, твой ответ не корректен. мне нужны id и сумма пополнения через пробел.')
        return


@dp.callback_query_handler(text="update_promo")
async def update_promo(call: types.CallbackQuery):
    cur_m.execute('SELECT promokods FROM promo WHERE id = %s', ('123123',))
    promokods = cur_m.fetchall()
    await call.message.answer(promokods)
    await set_passwords.password.set()


class set_passwords(StatesGroup):
    password = State()


@dp.message_handler(state=set_passwords.password)
async def set_promokods(message, state: FSMContext):
    promokods = message.text
    pay.update_promokods(conn_m, cur_m, promokods, '123123')
    await message.answer('промокоды обновленны, повелитель!')
    async with state.proxy() as data:
        data['password'] = message.text
    await state.finish()


@dp.message_handler(commands="start")
async def cmd_start_bot(message: types.Message):
    ID = str(message.from_user.id)
    user_channel_status = await bot.get_chat_member(chat_id='@scrappy_news', user_id=ID)
    if user_channel_status["status"] != 'left':
        pass
    else:
        await bot.send_message(message.from_user.id,
                               'Перед тем как продолжить, подпишитесь на наш канал с новостями @scrappy_news, затем введите /start')
        return
    if not pay.user_exists(conn_m, cur_m, ID):
        pay.add_user(conn_m, cur_m, ID)
    hallo_text = 'Привет, ' + str(message.from_user.username)
    await message.answer(hallo_text, reply_markup=keyboard.menu())


@dp.message_handler(Text(equals="Настройки"))
async def settings(message: types.Message):
    keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_menu = ["Назад", 'Изменить текст рассылки в WhatsApp']
    keyboard_menu.add(*buttons_menu)
    await message.answer('Что желаете изменить?', reply_markup=keyboard_menu)


@dp.message_handler(Text(equals="Изменить текст рассылки в WhatsApp"))
async def text_w(message: types.Message):
    await message.answer('Введите текст для рассылки в WhatsApp')
    await set_wtext.mess.set()


class set_wtext(StatesGroup):
    mess = State()


@dp.message_handler(state=set_wtext.mess)
async def get_wtext(message, state: FSMContext):
    text = message.text
    url_text = urllib.parse.quote(text)
    cur_m.execute('UPDATE wtext SET _text_ = %s WHERE id = %s', (url_text, message.from_user.id))
    await state.finish()
    await message.answer('настройки сохранены!', reply_markup=keyboard.menu())


@dp.message_handler(Text(equals="Тех поддержка"))
async def Teh_poddergka(message: types.Message):
    keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_menu = ["Начать парсинг", 'Тех поддержка']
    buttons_menu1 = ['Активировать подписку', "Пополнить баланс"]
    keyboard_menu.add(*buttons_menu)
    keyboard_menu.add(*buttons_menu1)
    await message.answer('По всем вопросам связанным с работой парсера обращайтесь:\n@U_gay\n@Alibi_na_vecher',
                         reply_markup=keyboard_menu)


@dp.message_handler(Text(equals="Пополнить баланс"))
async def sbor_deneg(message: types.Message):
    ID = message.from_user.id
    cur_m.execute('SELECT money FROM users WHERE id = %s LIMIT 1', (ID,))
    money = cur_m.fetchone()[0]
    text = f'тарифы SCRAPPYbot:\n1 день - 150 RUB\n3 дня - 350 RUB\n7 дней - 700 RUB\n30 дней - 2500 RUB\nна вашем балансе {money}'
    await message.answer(text, reply_markup=keyboard.pay())


@dp.callback_query_handler(text="set_promo")
async def set_promo(call: types.CallbackQuery):
    keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_menu = ["Назад"]
    keyboard_menu.add(*buttons_menu)
    await call.message.answer('Введите промокод', reply_markup=keyboard_menu)
    await set_promo.promo.set()


class set_promo(StatesGroup):
    promo = State()


@dp.message_handler(state=set_promo.promo)
async def get_promo(message, state: FSMContext):
    promo = message.text
    sail = pay.check_true_promo(cur_m, conn_m, promo)
    if message.text == 'Назад':
        async with state.proxy() as data:
            data['promo'] = message.text
        await state.finish()
    if not sail:
        await message.answer('Промокод не найден, введите коректный промокод', reply_markup=keyboard.back())
        return
    else:
        await message.answer(f'Промокод на пополнение +{sail}% активирован!', reply_markup=keyboard.menu())

    async with state.proxy() as data:
        data['promo'] = message.text
    await state.finish()


@dp.callback_query_handler(text="qiwi")
async def qiwi_pay(call: types.CallbackQuery):
    await call.message.answer('Введите сумму для пополнения')
    await save_url.url_for_pay.set()


class save_url(StatesGroup):
    url_for_pay = State()


@dp.message_handler(state=save_url.url_for_pay)
async def get_url_for_pay(message, state: FSMContext):
    try:
        message_money = int(message.text)
    except:
        await bot.send_message(message.from_user.id, 'введите число')
        return
    comment = str(message.from_user.id) + '_' + str(random.randint(1000, 9999))
    bill = p2p.bill(amount=message_money, lifetime=15, comment=comment)
    pay.add_check(conn_m, cur_m, message.from_user.id, message_money, bill.bill_id)
    keyboard_parsing = types.InlineKeyboardMarkup()
    keyboard_parsing.add(types.InlineKeyboardButton(text="Подтвердить перевод", callback_data="pay_chek"))
    await bot.send_message(message.from_user.id,
                           f'ссылка для пополнения баланса:\n{bill.pay_url}\nукажите коментарий "{comment}" при переводе',
                           reply_markup=keyboard_parsing)

    async with state.proxy() as data:
        data['url_for_pay'] = message.text
    await state.finish()


@dp.callback_query_handler(text="pay_chek")
async def pay_chek(call: types.CallbackQuery):
    bill = pay.get_bill(cur_m, conn_m, call.from_user.id)
    if bill:
        info = pay.get_check(cur_m, conn_m, bill)[0]
        if str(p2p.check(bill_id=bill).status) == 'PAID':
            user_money = pay.user_money(cur_m, conn_m, call.from_user.id)
            print(user_money)
            money = int(info[1]) + pay.sail(cur_m, call.from_user.id, int(info[1]))
            money_all = money + user_money
            pay.set_money(conn_m, cur_m, call.from_user.id, money_all)
            pay.delete_check(conn_m, cur_m, bill)
            await bot.send_message(call.from_user.id, f'{money} RUB добавленно на ваш баланс',
                                   reply_markup=keyboard.menu())
        else:
            keyboard_parsing = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
            buttons_parsing = ["Подтвердить перевод"]
            keyboard_parsing.add(*buttons_parsing)
            await bot.send_message(call.from_user.id, 'Оплата не прошла, попробуйте повторить подтверждение позже',
                                   reply_markup=keyboard_parsing)
    else:
        await bot.send_message(call.from_user.id, 'Чек не найден')


@dp.message_handler(Text(equals="Активировать подписку"))
async def sub_menu(message: types.Message):
    await message.answer('выберите подписку из списка', reply_markup=keyboard.sub())


@dp.callback_query_handler(text="sub_1")
async def sub_activate1(call: types.CallbackQuery):
    money = pay.user_money(cur_m, conn_m, call.from_user.id)
    if money >= 150:
        delta = timedelta(days=1)
        now = datetime.now()
        time_sub = now + delta
        money = money - 150
        pay.set_money(conn_m, cur_m, call.from_user.id, money)
        pay.set_time_sub(conn_m, cur_m, call.from_user.id, time_sub)
        await call.message.answer(f'подписка активированна!\nсрок окончания подписки {time_sub}')
    else:
        await call.message.answer(f'недостаточно средств.\nна вашем балансе {str(money)} RUB')


@dp.callback_query_handler(text="sub_2")
async def sub_activate2(call: types.CallbackQuery):
    money = pay.user_money(cur_m, conn_m, call.from_user.id)
    if money >= 350:
        delta = timedelta(days=3)
        now = datetime.now()
        time_sub = now + delta
        money = money - 350
        pay.set_money(conn_m, cur_m, call.from_user.id, money)
        pay.set_time_sub(conn_m, cur_m, call.from_user.id, time_sub)
        await call.message.answer(f'подписка активированна!\nсрок окончания подписки {time_sub}')
    else:
        await call.message.answer(f'недостаточно средств.\nна вашем балансе {str(money)} RUB')


@dp.callback_query_handler(text="sub_3")
async def sub_activate3(call: types.CallbackQuery):
    money = pay.user_money(cur_m, conn_m, call.from_user.id)
    if money >= 700:
        delta = timedelta(days=7)
        now = datetime.now()
        time_sub = now + delta
        money = money - 700
        pay.set_money(conn_m, cur_m, call.from_user.id, money)
        pay.set_time_sub(conn_m, cur_m, call.from_user.id, time_sub)
        await call.message.answer(f'подписка активированна!\nсрок окончания подписки {time_sub}')
    else:
        await call.message.answer(f'недостаточно средств.\nна вашем балансе {str(money)} RUB')


@dp.callback_query_handler(text="sub_4")
async def sub_activate4(call: types.CallbackQuery):
    money = pay.user_money(cur_m, conn_m, call.from_user.id)
    if money >= 2500:
        delta = timedelta(days=30)
        now = datetime.now()
        time_sub = now + delta
        money = money - 2500
        pay.set_money(conn_m, cur_m, call.from_user.id, money)
        pay.set_time_sub(conn_m, cur_m, call.from_user.id, time_sub)
        await call.message.answer(f'подписка активированна!\nсрок окончания подписки {time_sub}')
    else:
        await call.message.answer(f'недостаточно средств.\nна вашем балансе {str(money)} RUB')


@dp.message_handler(Text(equals="Начать парсинг"))
async def cmd_start_parser(message: types.Message):
    await message.answer("Выбери страну", reply_markup=keyboard.servis())


@dp.callback_query_handler(text="Ebay")
async def send_Ebay(call: types.CallbackQuery):
    await bot.delete_message(call.from_user.id, call.message.message_id)
    ID = call.from_user.id
    time_sub = pay.check_time_for_pay(cur_m, conn_m, call.from_user.id)
    keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_menu = ["Начать парсинг", 'Настройки']
    buttons_menu1 = ['Тех поддержка']
    buttons_menu2 = ['Активировать подписку', "Пополнить баланс"]
    keyboard_menu.add(*buttons_menu)
    keyboard_menu.add(*buttons_menu1)
    keyboard_menu.add(*buttons_menu2)
    if not time_sub:
        await call.message.answer('Для того чтобы продолжить, активируйте подписку', reply_markup=keyboard_menu)
    else:
        if datetime.now() < datetime.strptime(time_sub, '%Y-%m-%d %H:%M:%S.%f'):
            try:
                cur_m.execute('DELETE FROM strana WHERE id = %s', (ID,))
            except:
                conn_m.rollback()
            cur_m.execute('INSERT INTO strana (ID, cuntree) VALUES (%s, %s)', (ID, 'ebay'))
            conn_m.commit()
            await get_start(call)
        else:
            await call.message.answer('Для того чтобы продолжить, активируйте подписку', reply_markup=keyboard_menu)


@dp.callback_query_handler(text="Bolha")
async def send_Bolha(call: types.CallbackQuery):
    await bot.delete_message(call.from_user.id, call.message.message_id)
    ID = call.from_user.id
    time_sub = pay.check_time_for_pay(cur_m, conn_m, call.from_user.id)
    keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_menu = ["Начать парсинг", 'Настройки']
    buttons_menu1 = ['Тех поддержка']
    buttons_menu2 = ['Активировать подписку', "Пополнить баланс"]
    keyboard_menu.add(*buttons_menu)
    keyboard_menu.add(*buttons_menu1)
    keyboard_menu.add(*buttons_menu2)
    if time_sub is None:
        await call.message.answer('Для того чтобы продолжить, активируйте подписку', reply_markup=keyboard_menu)
        return
    if not time_sub:
        await call.message.answer('Для того чтобы продолжить, активируйте подписку', reply_markup=keyboard_menu)
    else:
        if datetime.now() < datetime.strptime(time_sub, '%Y-%m-%d %H:%M:%S.%f'):
            try:
                cur_m.execute('DELETE FROM strana WHERE id = %s', (ID,))
            except:
                conn_m.rollback()
            cur_m.execute('INSERT INTO strana (ID, cuntree) VALUES (%s, %s)', (ID, 'bolha'))
            conn_m.commit()
            await get_start(call)
        else:
            await call.message.answer('Для того чтобы продолжить, активируйте подписку', reply_markup=keyboard_menu)


@dp.callback_query_handler(text="Gumtree")
async def send_Gumtree(call: types.CallbackQuery):
    await bot.delete_message(call.from_user.id, call.message.message_id)
    ID = call.from_user.id
    time_sub = pay.check_time_for_pay(cur_m, conn_m, call.from_user.id)
    keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_menu = ["Начать парсинг", 'Настройки']
    buttons_menu1 = ['Тех поддержка']
    buttons_menu2 = ['Активировать подписку', "Пополнить баланс"]
    keyboard_menu.add(*buttons_menu)
    keyboard_menu.add(*buttons_menu1)
    keyboard_menu.add(*buttons_menu2)
    if not time_sub:
        await call.message.answer('Для того чтобы продолжить, активируйте подписку', reply_markup=keyboard_menu)
    else:
        if datetime.now() < datetime.strptime(time_sub, '%Y-%m-%d %H:%M:%S.%f'):
            try:
                cur_m.execute('DELETE FROM strana WHERE id = %s', (ID,))
            except:
                conn_m.rollback()
                cur_m.execute('INSERT INTO strana (ID, cuntree) VALUES (%s, %s)', (ID, 'gumtree'))
                conn_m.commit()
                await get_start(call)
        else:
            await call.message.answer('Для того чтобы продолжить, активируйте подписку', reply_markup=keyboard_menu)


@dp.callback_query_handler(text="OLX")
async def send_OLXpl(call: types.CallbackQuery):
    await bot.delete_message(call.from_user.id, call.message.message_id)
    ID = call.from_user.id
    time_sub = pay.check_time_for_pay(cur_m, conn_m, call.from_user.id)
    message = call.message
    keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_menu = ["Начать парсинг", 'Настройки']
    buttons_menu1 = ['Тех поддержка']
    buttons_menu2 = ['Активировать подписку', "Пополнить баланс"]
    keyboard_menu.add(*buttons_menu)
    keyboard_menu.add(*buttons_menu1)
    keyboard_menu.add(*buttons_menu2)
    if not time_sub:
        await call.message.answer('Для того чтобы продолжить, активируйте подписку', reply_markup=keyboard_menu)
        return
    else:
        if datetime.now() < datetime.strptime(time_sub, '%Y-%m-%d %H:%M:%S.%f'):
            try:
                cur_m.execute('SELECT tokens FROM olxpl WHERE id = %s', (ID,))
                tokens = cur_m.fetchone()[0]
                tokens = re.findall('\S+', str(tokens))
                if tokens[0] == 'None':
                    message = call.message
                    keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
                    buttons_menu = ["Назад", "Ввести токены OLXpl"]
                    keyboard_menu.add(*buttons_menu)
                    await message.answer("требуется токен", reply_markup=keyboard_menu)
                    return
            except:
                cur_m.execute('DELETE FROM olxpl WHERE id = %s', (ID,))
                cur_m.execute('INSERT INTO olxpl (id) VALUES (%s)', (ID,))
                conn_m.commit()
                keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
                buttons_menu = ["Назад", "Ввести токены OLXpl"]
                keyboard_menu.add(*buttons_menu)
                message = call.message
                await message.answer("требуется токен", reply_markup=keyboard_menu)
                return
            try:
                cur_m.execute('DELETE FROM strana WHERE id = %s', (ID,))
            except:
                conn_m.rollback()
            cur_m.execute('INSERT INTO strana (ID, cuntree) VALUES (%s, %s)', (ID, 'olx.pl'))
            conn_m.commit()
            await get_start(call)
        else:
            await message.answer('Для того чтобы продолжить, активируйте подписку', reply_markup=keyboard_menu)


@dp.callback_query_handler(text="OLXua")
async def send_OLXua(call: types.CallbackQuery):
    await bot.delete_message(call.from_user.id, call.message.message_id)
    ID = call.from_user.id
    time_sub = pay.check_time_for_pay(cur_m, conn_m, call.from_user.id)
    message = call.message
    keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_menu = ["Начать парсинг", 'Настройки']
    buttons_menu1 = ['Тех поддержка']
    buttons_menu2 = ['Активировать подписку', "Пополнить баланс"]
    keyboard_menu.add(*buttons_menu)
    keyboard_menu.add(*buttons_menu1)
    keyboard_menu.add(*buttons_menu2)
    if not time_sub:
        await call.message.answer('Для того чтобы продолжить, активируйте подписку', reply_markup=keyboard_menu)
        return
    else:
        if datetime.now() < datetime.strptime(time_sub, '%Y-%m-%d %H:%M:%S.%f'):
            try:
                cur_m.execute('SELECT tokens FROM olxua WHERE id = %s', (ID,))
                tokens = cur_m.fetchone()[0]
                tokens = re.findall('\S+', str(tokens))
                if tokens[0] == 'None':
                    message = call.message
                    keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
                    buttons_menu = ["Назад", "Ввести токены OLX.ua"]
                    keyboard_menu.add(*buttons_menu)
                    await message.answer("требуется токен", reply_markup=keyboard_menu)
                    return
            except:
                cur_m.execute('DELETE FROM olxua WHERE id = %s', (ID,))
                cur_m.execute('INSERT INTO olxua (id) VALUES (%s)', (ID,))
                conn_m.commit()
                keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
                buttons_menu = ["Назад", "Ввести токены OLX.ua"]
                keyboard_menu.add(*buttons_menu)
                message = call.message
                await message.answer("требуется токен", reply_markup=keyboard_menu)
                return
            try:
                cur_m.execute('DELETE FROM strana WHERE id = %s', (ID,))
            except:
                conn_m.rollback()
            cur_m.execute('INSERT INTO strana (ID, cuntree) VALUES (%s, %s)', (ID, 'olx.ua'))
            conn_m.commit()
            await get_start(call)
        else:
            await message.answer('Для того чтобы продолжить, активируйте подписку', reply_markup=keyboard_menu)


@dp.callback_query_handler(text="wallapop")
async def send_Bolha(call: types.CallbackQuery):
    await bot.delete_message(call.from_user.id, call.message.message_id)
    ID = call.from_user.id
    time_sub = pay.check_time_for_pay(cur_m, conn_m, call.from_user.id)
    keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_menu = ["Начать парсинг", 'Настройки']
    buttons_menu1 = ['Тех поддержка']
    buttons_menu2 = ['Активировать подписку', "Пополнить баланс"]
    keyboard_menu.add(*buttons_menu)
    keyboard_menu.add(*buttons_menu1)
    keyboard_menu.add(*buttons_menu2)
    if time_sub is None:
        await call.message.answer('Для того чтобы продолжить, активируйте подписку', reply_markup=keyboard_menu)
        return
    if not time_sub:
        await call.message.answer('Для того чтобы продолжить, активируйте подписку', reply_markup=keyboard_menu)
    else:
        if datetime.now() < datetime.strptime(time_sub, '%Y-%m-%d %H:%M:%S.%f'):
            try:
                cur_m.execute('DELETE FROM strana WHERE id = %s', (ID,))
            except:
                conn_m.rollback()
            cur_m.execute('INSERT INTO strana (ID, cuntree) VALUES (%s, %s)', (ID, 'wallapop'))
            conn_m.commit()
            await get_start(call)
        else:
            await call.message.answer('Для того чтобы продолжить, активируйте подписку', reply_markup=keyboard_menu)


@dp.callback_query_handler(text="tutti")
async def send_Bolha(call: types.CallbackQuery):
    await bot.delete_message(call.from_user.id, call.message.message_id)
    ID = call.from_user.id
    time_sub = pay.check_time_for_pay(cur_m, conn_m, call.from_user.id)
    keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_menu = ["Начать парсинг", 'Настройки']
    buttons_menu1 = ['Тех поддержка']
    buttons_menu2 = ['Активировать подписку', "Пополнить баланс"]
    keyboard_menu.add(*buttons_menu)
    keyboard_menu.add(*buttons_menu1)
    keyboard_menu.add(*buttons_menu2)
    if time_sub is None:
        await call.message.answer('Для того чтобы продолжить, активируйте подписку', reply_markup=keyboard_menu)
        return
    if not time_sub:
        await call.message.answer('Для того чтобы продолжить, активируйте подписку', reply_markup=keyboard_menu)
    else:
        if datetime.now() < datetime.strptime(time_sub, '%Y-%m-%d %H:%M:%S.%f'):
            try:
                cur_m.execute('DELETE FROM strana WHERE id = %s', (ID,))
            except:
                conn_m.rollback()
            cur_m.execute('INSERT INTO strana (ID, cuntree) VALUES (%s, %s)', (ID, 'tutti'))
            conn_m.commit()
            await get_start(call)
        else:
            await call.message.answer('Для того чтобы продолжить, активируйте подписку', reply_markup=keyboard_menu)


@dp.message_handler(Text(equals="Остановить парсинг"))
async def send_stop(message: types.Message):
    ID = message.from_user.id
    cur_m.execute('UPDATE stoped SET stop = %s WHERE id = %s', ('yes', ID))
    conn_m.commit()


class data_parser(StatesGroup):
    URL = State()
    DATA = State()
    opit_ = State()
    PAGENATION_ot = State()
    PAGENATION_do = State()


async def get_start(call):
    message = call.message
    await data_parser.URL.set()
    keyboard_data = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_data = ['Назад']
    keyboard_data.add(*buttons_data)
    await message.answer("введите ссылку категории для парсинга", reply_markup=keyboard_data)


@dp.message_handler(state=data_parser.URL)
async def get_URL(message, state: FSMContext):
    if message.text == 'Назад':
        await state.finish()
        await menu(message)
        return
    ID = message.from_user.id
    cur_m.execute('SELECT cuntree FROM strana WHERE id = %s LIMIT 1', (ID,))
    cuntree = cur_m.fetchone()[0]
    if cuntree.find('olx') != -1:
        cuntree = 'olx'
    if message.text.find(cuntree) != -1:
        pass
    else:
        await message.answer(
            f"Ссылка не содержит домена выбранного вами сайта.\nCсылка должна начинаться с https://www.{cuntree}",
            reply_markup=keyboard.back())
        return
    try:
        URL = message.text
        html = get_html(URL)
        if html.status_code != 200:
            await message.answer("ссылка не подходит, введите другую")
            return
    except Exception as ex:
        print(ex)
        await message.answer("ссылка не подходит, введите другую")
        return
    async with state.proxy() as data:
        data['URL'] = message.text
    cur_m.execute('SELECT data FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    try:
        DATA_bd = cur_m.fetchone()[0]
    except:
        now = datetime.now()
        DATA_bd = str(now.strftime("%d.%m.%Y"))
    if re.fullmatch('....-..-..', DATA_bd):
        day = re.findall('[0-9]+.[0-9]+.([0-9]+)', DATA_bd)
        mesyac = re.findall('[0-9]+.([0-9]+).[0-9]+', DATA_bd)
        year = re.findall('([0-9]+).[0-9]+.[0-9]+', DATA_bd)
        DATA_bd = day[0] + '.' + mesyac[0] + '.' + year[0]
    elif re.fullmatch('..-..-....', DATA_bd):
        year = re.findall('[0-9]+.[0-9]+.([0-9]+)', DATA_bd)
        mesyac = re.findall('[0-9]+.([0-9]+).[0-9]+', DATA_bd)
        day = re.findall('([0-9]+).[0-9]+.[0-9]+', DATA_bd)
        DATA_bd = day[0] + '.' + mesyac[0] + '.' + year[0]
    keyboard_data = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_data = [str(DATA_bd), 'Назад']
    keyboard_data.add(*buttons_data)
    await data_parser.next()
    await message.answer(
        "введите дату размещения искомых обьявлений в формате: ДД.ММ.ГГГГ например(18.12.2021) или напишите 'No' если не хотите учитывать этот параметр",
        reply_markup=keyboard_data)


@dp.message_handler(state=data_parser.DATA)
async def get_DATA(message, state: FSMContext):
    ID = message.from_user.id
    if message.text == 'Назад':
        await state.finish()
        await menu(message)
        return
    if re.fullmatch('[0-9]{2}.[0-9]{2}.[0-9]{4}', message.text) or message.text == 'No':
        pass
    else:
        await message.answer("Введите коректный ответ", reply_markup=keyboard.back())
        return
    async with state.proxy() as data:
        data['DATA'] = message.text
    cur_m.execute('SELECT opit FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    try:
        opit_bd = str(cur_m.fetchone()[0])
    except:
        opit_bd = '3'
    keyboard_opit = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_opit = [opit_bd, 'Назад']
    keyboard_opit.add(*buttons_opit)
    await data_parser.next()
    await message.answer("максимально допустимое количество сделок у продавца на сайте?", reply_markup=keyboard_opit)


@dp.message_handler(state=data_parser.opit_)
async def get_opit(message, state: FSMContext):
    ID = message.from_user.id
    if message.text == 'Назад':
        await state.finish()
        await menu(message)
        return
    try:
        int(message.text)
    except:
        await message.answer("введите число")
        return
    async with state.proxy() as data:
        data['opit_'] = int(message.text)
    await data_parser.next()
    cur_m.execute('SELECT cuntree FROM strana WHERE id = %s LIMIT 1', (ID,))
    cuntree = cur_m.fetchone()[0]
    if cuntree == 'wallapop':
        ID = message.from_user.id
        cur_m.execute('DELETE FROM num WHERE id = %s', (ID,))
        cur_m.execute(
            'INSERT INTO num (id, all_count, success, error1, error2, error3, error4, page, call) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)',
            (ID, 0, 0, 0, 0, 0, 0, 0, 0))
        cur_m.execute('DELETE FROM bd_bot WHERE id = %s', (ID,))
        cur_m.execute('INSERT INTO bd_bot (id, url, data, opit, ot, pdo) VALUES(%s, %s, %s, %s, %s, %s)',
                      (ID, md.text(md.text(data['URL'])), md.text(md.text(data['DATA'])),
                       md.text(md.text(data['opit_'])), 0, 0))
        conn_m.commit()
        await state.finish()
        countree_code = re.findall('https://(.{2})', md.text(md.text(data['URL'])))[0]
        print(countree_code)
        if countree_code == 'uk':
            await wallapop_uk.start_parsing(message, conn_m)
        elif countree_code == 'it':
            await wallapop_it.start_parsing(message, conn_m)
        return
    cur_m.execute('SELECT ot FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    try:
        PAGENATION_ot_bd = str(cur_m.fetchone()[0])
    except:
        PAGENATION_ot_bd = '1'
    keyboard_ot = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_ot = [PAGENATION_ot_bd, 'Назад']
    keyboard_ot.add(*buttons_ot)
    await message.answer("с какой страницы начать парсинг?", reply_markup=keyboard_ot)


@dp.message_handler(state=data_parser.PAGENATION_ot)
async def get_OT(message, state: FSMContext):
    ID = message.from_user.id
    if message.text == 'Назад':
        await state.finish()
        await menu(message)
        return
    try:
        int(message.text)
    except:
        await message.answer("введите число")
        return
    async with state.proxy() as data:
        data['PAGENATION_ot'] = int(message.text)
    cur_m.execute('SELECT pdo FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    try:
        PAGENATION_do_bd = cur_m.fetchone()[0]
        PAGENATION_do_bd = str(PAGENATION_do_bd - 1)
    except:
        PAGENATION_do_bd = '15'
    keyboard_do = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_do = [PAGENATION_do_bd, 'Назад']
    keyboard_do.add(*buttons_do)
    await data_parser.next()
    await message.answer("на какой закончить?", reply_markup=keyboard_do)


@dp.message_handler(state=data_parser.PAGENATION_do)
async def get_DO(message, state: FSMContext):
    if message.text == 'Назад':
        await state.finish()
        await menu(message)
        return
    try:
        int(message.text)
    except:
        await message.answer("введите число")
        return
    async with state.proxy() as data:
        data['PAGENATION_do'] = int(message.text) + 1

    ID = message.from_user.id
    cur_m.execute('DELETE FROM num WHERE id = %s', (ID,))
    cur_m.execute(
        'INSERT INTO num (id, all_count, success, error1, error2, error3, error4, page, call) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)',
        (ID, 0, 0, 0, 0, 0, 0, 0, 0))
    cur_m.execute('DELETE FROM bd_bot WHERE id = %s', (ID,))
    cur_m.execute('INSERT INTO bd_bot (id, url, data, opit, ot, pdo) VALUES(%s, %s, %s, %s, %s, %s)',
                  (ID, md.text(md.text(data['URL'])), md.text(md.text(data['DATA'])), md.text(md.text(data['opit_'])),
                   md.text(md.text(data['PAGENATION_ot'])), md.text(md.text(data['PAGENATION_do']))))
    conn_m.commit()
    await state.finish()

    ID = message.from_user.id
    cur_m.execute('SELECT cuntree FROM strana WHERE id = %s LIMIT 1', (ID,))
    cuntree = cur_m.fetchone()[0]
    cur_m.execute('SELECT url FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    URL_bd = cur_m.fetchone()[0]
    if URL_bd.find('olx') != -1:
        cuntree = re.findall('https://.+?.olx.(.{2}).+', URL_bd)[0]
        print(cuntree)

    print(cuntree)
    if cuntree == 'ebay':
        await ebay.start_parsing(message, conn_m)
    elif cuntree == 'bolha':
        await bolha.start_parsing(message, conn_m)
    elif cuntree == 'tutti':
        await tutti_ch.start_parsing(message, conn_m)
    elif cuntree == 'gumtree':
        await gumtree.start_parsing(message, conn_m)
    elif cuntree == 'pl':
        result = await olx.start_parsing(message, conn_m)
        if result == 'true':
            keyboard_token = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
            buttons_token = ["OLX.pl Ввести токены", 'Назад']
            keyboard_token.add(*buttons_token)
            await message.answer('требуется токен', reply_markup=keyboard_token)
    elif cuntree == 'ua':
        result = await olx_ua.start_parsing(message, conn_m)
        if result == 'true':
            keyboard_token = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
            buttons_token = ["OLX.ua Ввести токены", 'Назад']
            keyboard_token.add(*buttons_token)
            await message.answer('требуется токен', reply_markup=keyboard_token)


class data_token(StatesGroup):
    URL = State()


@dp.message_handler(Text(equals="Ввести токены OLXpl"))
async def start_token(message: types.Message):
    await data_token.URL.set()
    await message.answer('введите токены через пробел без запятых')


@dp.message_handler(state=data_token.URL)
async def get_tokens(message, state: FSMContext):
    ID = message.from_user.id
    tokens_message = message.text
    tokens_re = re.findall('\s*?(\S{40}),*?', tokens_message)
    counts = len(tokens_re)
    tokens = ''
    for token in tokens_re:
        tokens = tokens + ' ' + token
        print(token)
    cur_m.execute('UPDATE olxpl SET tokens = %s WHERE id = %s', (tokens, ID))
    conn_m.commit()
    await state.finish()
    keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_menu = ["Начать парсинг", 'Ввести токены OLX.pl']  # , 'Парсинг+']
    keyboard_menu.add(*buttons_menu)
    buttons_menu1 = ['Назад']  # , 'Парсинг+']
    keyboard_menu.add(*buttons_menu1)
    text = str(counts) + ' токенов добавленно'
    await message.answer(text, reply_markup=keyboard_menu)


class data_ua_token(StatesGroup):
    URL = State()


@dp.message_handler(Text(equals="Ввести токены OLX.ua"))
async def start_token(message: types.Message):
    await data_ua_token.URL.set()
    await message.answer('введите токены через пробел без запятых')


@dp.message_handler(state=data_ua_token.URL)
async def get_tokens(message, state: FSMContext):
    id = message.from_user.id
    tokens_message = message.text
    tokens_re = re.findall('\s*?(\S{40}),*?', tokens_message)
    counts = len(tokens_re)
    tokens = ''
    for token in tokens_re:
        tokens = tokens + ' ' + token
        print(token)
    cur_m.execute('UPDATE olxua SET tokens = %s WHERE id = %s', (tokens, id))
    conn_m.commit()
    await state.finish()
    keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_menu = ["Начать парсинг", 'Ввести токены OLX.ua']  # , 'Парсинг+']
    keyboard_menu.add(*buttons_menu)
    buttons_menu1 = ['Назад']  # , 'Парсинг+']
    keyboard_menu.add(*buttons_menu1)
    text = str(counts) + ' токенов добавленно'
    await message.answer(text, reply_markup=keyboard_menu)


def get_html(url, params=''):
    response = requests.get(url, headers=HEADERS, params=params)
    return response


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
