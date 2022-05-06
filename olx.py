from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram import Bot, Dispatcher, types
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import requests
import re
import psycopg2
import asyncio
import aiohttp
import random
import ast
import keyboard
import config

bot = Bot(config.token_bot)
dp = Dispatcher(bot, storage=MemoryStorage())

HOST = 'https://www.olx.pl'

ua = UserAgent()
HEADERS = {'User-Agent': ua.chrome}

month = {'sty': '01', 'lut': '02', 'mar': '03', 'kwi': '04', 'maj': '05', 'cze': '06', 'lip': '07', 'sie': '08',
         'wrz': '09', 'paz': '10', 'lis': '11', 'gru': '12'}


async def start_parsing(message, conn):
    cur = conn.cursor()
    ID = message.from_user.id
    cur.execute('UPDATE stoped SET stop = %s WHERE id = %s', ('no', ID))
    conn.commit()
    cur.execute('SELECT url FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    URL_bd = cur.fetchone()[0]
    cur.execute('SELECT ot FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    PAGENATION_ot_bd = cur.fetchone()[0]
    cur.execute('SELECT pdo FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    PAGENATION_do_bd = cur.fetchone()[0]
    if PAGENATION_do_bd > 50:
        PAGENATION_do_bd = 50
    conn.commit()

    keyboard_stop = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_stop = ["Остановить парсинг"]
    keyboard_stop.add(*buttons_stop)
    await bot.send_message(ID, 'Процесс парсинга запущен. Это может занять некоторое время', reply_markup=keyboard_stop)

    dabl_url = []
    for page in range(PAGENATION_ot_bd, PAGENATION_do_bd):
        cur.execute('SELECT stop FROM stoped WHERE id = %s LIMIT 1', (ID,))
        stop_bd = cur.fetchone()[0]
        if stop_bd == 'yes':
            pages_print = 'Парсинг остановлен на странице ' + str(page)
            await bot.send_message(message.from_user.id, pages_print)
            break
        url = URL_bd + '/?page=' + str(page)
        # pages_print='парсим страницу: '+str(page)
        # await bot.send_message(message.from_user.id, pages_print)
        # print(page)
        result = await first_step(url, str(message.from_user.id), conn, cur, dabl_url)
        if result == 'tru':
            return
    await end(message, cur, ID)


async def fetch(session, url, proxy):
    HEADERS = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'
    }
    async with session.get(url, headers=HEADERS, ssl=False, proxy=proxy) as resp:
        return await resp.text()


async def first_step(url, ID, conn, cur, dabl_url):
    cur.execute('SELECT data FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    DATA_bd = cur.fetchone()[0]
    now = datetime.now()
    year = re.findall('(....)-.+', str(now))
    HEADERS = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'
    }
    async with aiohttp.ClientSession() as session_loop:
        async with session_loop.get(url, headers=HEADERS, ssl=False) as resp:
            html_links = await resp.text()
        items_list = []
        items_dict = {}
        soup = BeautifulSoup(html_links, 'lxml')
        items = soup.find_all('tr', class_="wrap")
        for item in items:
            cur.execute('SELECT stop FROM stoped WHERE id = %s LIMIT 1', (ID,))
            stop_bd = cur.fetchone()[0]
            if stop_bd == 'yes':
                break
            cur.execute('SELECT all_count FROM num WHERE id = %s LIMIT 1', (ID,))
            all_count_ = int(cur.fetchone()[0]) + 1
            cur.execute('UPDATE num SET all_count = %s WHERE id = %s', (all_count_, ID))
            if DATA_bd == 'No':
                data_re = 'No'
            else:
                data_all = item.find_all('small', class_='breadcrumb x-normal')
                count = 0
                for data in data_all:
                    count += 1
                    if count == 3:
                        dat = re.findall('i>(.+)<', str(data))
                        data = dat[0]
                if data.find('wczoraj') != -1:
                    delta = timedelta(1)
                    data_all = now - delta
                    data_re = re.findall('(\S+) .+', str(data_all))
                    data_re = data_re[0]
                else:
                    if data.find('dzisiaj') != -1:
                        data_re = str(now.strftime("%d.%m.%Y"))
                    else:
                        da = re.findall('([0-9]+).+', str(data))
                        day = da[0]
                        if len(day) == 1:
                            day = '0' + day
                        mon_re = re.findall('.+  (.+)', str(data))
                        mon = month[mon_re[0]]
                        data_re = day + '.' + mon + '.' + year[0]
            if DATA_bd == data_re:
                links = item.find('a', class_="marginright5").get('href')
                link = re.sub('#.+', '', links)
                try:
                    product_img = item.find('img', class_="fleft").get('src')
                    title = item.find('img', class_="fleft").get('alt')
                except:
                    product_img = 'None'
                    title = 'tovar bez kartinki'
                try:
                    price = item.find('p', class_="price").get_text(strip=True)
                except:
                    price = 'None'
                    print('prise_error')
                item_dict = {link: {
                    'link': links,
                    'img': product_img,
                    'title': title,
                    'price': price,
                    'data': data_re
                }}
                items_dict.update(item_dict)
                items_list.append(links)
            else:
                cur.execute('SELECT error2 FROM num WHERE id = %s LIMIT 1', (ID,))
                error2_ = int(cur.fetchone()[0]) + 1
                cur.execute('UPDATE num SET error2 = %s WHERE id = %s', (error2_, ID))
        print('first_step compleat {0}'.format(len(items_list)))
        result = await second_step(items_list, items_dict, ID, session_loop, conn, cur, dabl_url)
        return result


dabl_url = []


async def second_step(items_list, items_dict, ID, session, conn, cur, dabl_url):
    cur.execute('SELECT proxy_list FROM proxy WHERE id = %s', ('123123',))
    proxy_list = cur.fetchall()[0][0]
    proxy_list = ast.literal_eval(proxy_list)
    proxy = random.choice(proxy_list)
    loop = asyncio.get_event_loop()
    async with aiohttp.ClientSession(loop=loop) as session_loop:
        html_links = await asyncio.gather(*[fetch(session_loop, url, proxy) for url in items_list])
    items_dict1 = {}
    items_list1 = []
    for item in html_links:
        cur.execute('SELECT stop FROM stoped WHERE id = %s LIMIT 1', (ID,))
        stop_bd = cur.fetchone()[0]
        if stop_bd == 'yes':
            break
        st_index = item.find('window.__PRERENDERED_STATE__')
        en_index = item.find('children')
        line = item[st_index:en_index]
        phone_index = line.find('phone')
        phone = line.find('true', phone_index, phone_index + 20)
        soup = BeautifulSoup(item, 'lxml')
        if phone != -1:
            try:
                link = soup.find('a', class_="css-1qj8w5r").get('href')
                if not link.startswith('https'):
                    link = HOST + link
                else:
                    continue
            except:
                print('link_error')
                continue
            # id_account = re.findall('"sellerId": (.+?),', item)
            try:
                phone_id = re.findall('id...(\d{9})', item)
                phone_id = phone_id[0]
            except:
                print('phone_except')
                continue
            name = soup.find('h2', class_='css-u8mbra-Text eu5v0x0').get_text(strip=True).lower()
            key_for_list = soup.find('link', id="ssr_canonical").get('href')
            items_dict[key_for_list].update({
                'phone_id': phone_id
            })
            dict_ = {name: items_dict[key_for_list]}
            items_dict1.update(dict_)
            items_list1.append(link)
        else:
            cur.execute('SELECT error1 FROM num WHERE id = %s LIMIT 1', (ID,))
            error1_ = int(cur.fetchone()[0]) + 1
            cur.execute('UPDATE num SET error1 = %s WHERE id = %s', (error1_, ID))
    print('second_step compleat {0}'.format(len(items_list1)))
    result = await third_step(items_list1, items_dict1, ID, conn, cur, dabl_url)
    return result


async def get_phone(id_, token):
    print(id_)
    try:
        phone = requests.get(f'https://www.olx.pl/api/v1/offers/{id_}/limited-phones/',
                             headers={'authorization': f'Bearer {token}'}).json()['data']['phones'][0]
        return phone
    except:
        return 'none'


async def third_step(items_list1, items_dict1, ID, conn, cur, dabl_url):
    cur.execute('SELECT opit FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    opit_bd = cur.fetchone()[0]
    cur.execute('SELECT _text_ FROM wtext WHERE id = %s LIMIT 1', (ID,))
    url_text = cur.fetchone()[0]
    if url_text is None:
        url_text = 'cze%C5%9B%C4%87%2C+czy+ta+reklama+jest+nadal+aktywna%3F'
    cur.execute('SELECT proxy_list FROM proxy WHERE id = %s', ('123123',))
    proxy_list = cur.fetchall()[0][0]
    proxy_list = ast.literal_eval(proxy_list)
    proxy = random.choice(proxy_list)
    loop = asyncio.get_event_loop()
    async with aiohttp.ClientSession(loop=loop) as session_loop:
        html_links = await asyncio.gather(*[fetch(session_loop, url, proxy) for url in items_list1])

    cur.execute('SELECT tokens FROM olxpl WHERE id = %s LIMIT 1', (ID,))
    tokens = cur.fetchone()[0]
    tokens = re.findall('\S+', str(tokens))
    for item in html_links:
        cur.execute('SELECT stop FROM stoped WHERE id = %s LIMIT 1', (ID,))
        stop_bd = cur.fetchone()[0]
        if stop_bd == 'yes':
            break
        soup = BeautifulSoup(item, 'lxml')
        try:
            how_many = soup.find('div', class_="locationlinks margintop10").get_text(strip=True)
            how_many_re = re.findall('([0-9]+)', how_many)
            how_many_tovar = int(how_many_re[0])
        except:
            print('how_many_error')
            continue
        name = soup.find('h2', class_="search-user-profile__user-name").get_text(strip=True).lower()
        link = name
        if opit_bd >= how_many_tovar:
            keyerror = 'false'
            zero_tokens = 'no'
            while True:
                try:
                    phone = await get_phone(items_dict1[link]['phone_id'], tokens[0])
                    if phone != 'none':
                        phone = re.sub("[^0-9]", "", phone)
                except KeyError:
                    keyerror = 'true'
                    break
                if phone != 'none':
                    break
                else:
                    try:
                        tokens.pop(0)
                        token = tokens[0]
                    except:
                        keyboard_menu = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
                        buttons_menu = ["Начать парсинг", 'Ввести токены OLX.pl']  # , 'Парсинг+']
                        keyboard_menu.add(*buttons_menu)
                        buttons_menu1 = ['Назад']  # , 'Парсинг+']
                        keyboard_menu.add(*buttons_menu1)
                        await bot.send_message(ID, 'добавьте токен', reply_markup=keyboard_menu)
                        cur.execute('UPDATE olxpl SET tokens = %s WHERE id = %s', ('', ID))
                        conn.commit()
                        zero_tokens = 'tru'
                        break
            if keyerror == 'true':
                print('keyerror')
                continue
            if zero_tokens == 'tru':
                return zero_tokens
            try:
                if dabl_url.count(str(items_dict1[link]['link'])) > 0:
                    continue
                else:
                    dabl_url.append(str(items_dict1[link]['link']))
            except:
                continue
            cur.execute('SELECT success FROM num WHERE id = %s LIMIT 1', (ID,))
            success_ = int(cur.fetchone()[0]) + 1
            cur.execute('UPDATE num SET success = %s WHERE id = %s', (success_, ID))
            message_text = ('<code>' + str(items_dict1[link]['title']) + '</code>' + '\nссылка: ' + str(
                items_dict1[link]['link']) + '\nцена: ' + '<code>' + str(
                items_dict1[link]['price']) + '</code>' + '\nкартинка: ' + str(
                items_dict1[link]['img']) + '\nколичество сделок у продавца: ' + str(
                how_many_tovar) + '\nдата размещения объявления: ' + str(
                items_dict1[link]['data']) + '\nтелефон: ' + '<code>44' + str(phone)) + '</code>'
            buttons = [
                types.InlineKeyboardButton(text='Написать в whatsap',
                                           url='https://wa.me/{0}?text={1}+{2}'.format(phone, url_text,
                                                                                       items_dict1[link]['link']))
            ]
            keyboard = types.InlineKeyboardMarkup()
            keyboard.add(*buttons)
            await bot.send_message(ID, message_text, parse_mode="HTML", reply_markup=keyboard)
        else:
            cur.execute('SELECT error3 FROM num WHERE id = %s LIMIT 1', (ID,))
            error3_ = int(cur.fetchone()[0]) + 1
            cur.execute('UPDATE num SET error3 = %s WHERE id = %s', (error3_, ID))
    return dabl_url


async def end(message, cur, ID):
    cur.execute('SELECT stop FROM stoped WHERE id = %s LIMIT 1', (ID,))
    stop_bd = cur.fetchone()[0]
    if stop_bd == 'yes':
        await bot.send_message(message.from_user.id, 'Парсинг остановлен', reply_markup=keyboard.menu())
        return
    cur.execute('SELECT success FROM num WHERE id = %s LIMIT 1', (ID,))
    success_ = cur.fetchone()[0]
    cur.execute('SELECT error1 FROM num WHERE id = %s LIMIT 1', (ID,))
    error1_ = cur.fetchone()[0]
    cur.execute('SELECT error2 FROM num WHERE id = %s LIMIT 1', (ID,))
    error2_ = cur.fetchone()[0]
    cur.execute('SELECT error3 FROM num WHERE id = %s LIMIT 1', (ID,))
    error3_ = cur.fetchone()[0]
    cur.execute('SELECT all_count FROM num WHERE id = %s LIMIT 1', (ID,))
    all_count_ = cur.fetchone()[0]
    end_sey = 'парсинг завершен \n' + \
              'всего спарсили: ' + str(all_count_) + ' объявлений\n' + \
              'подошло ' + str(success_) + ' обьявлений\n' + \
              'номер отсутствует у ' + str(error1_) + ' обьявлений\n' + \
              'не подходит по дате размещения ' + str(error2_) + ' обьявлений\n' + \
              'сделок слишком много у ' + str(error3_) + ' обьявлений\n'
    await bot.send_message(message.from_user.id, end_sey, reply_markup=keyboard.menu())
