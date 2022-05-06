# aiogram бот тг
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram import Bot, Dispatcher, types

import time

# красивый суп и работа с html и https
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

# регулярочки
import re

# модуль базы данных
# import sqlite3
import psycopg2

# asynхронность
import asyncio
import aiohttp
from aiohttp_proxy import ProxyConnector

# pikрандом
import random
from random import randint

# из str в list
import ast

import config
import keyboard

bot = Bot(config.token_bot)
dp = Dispatcher(bot, storage=MemoryStorage())

HOST = 'https://uk.wallapop.com'
HOST_ITEM = 'https://uk.wallapop.com/item/'
ua = UserAgent()
HEADERS = {'User-Agent': ua.chrome}
ctg_dict = {
    'cars': ['100', '5', '051253ff-b3ba-49a7-97dd-70e81d157f24'],
    'motorbikes': ['14000', '3', 'fa1079e4-73d2-4c41-b774-660050e5dfa7'],
    'motors-and-accessories': ['12800', '5', 'e5f83efa-12b2-48bd-a536-a6fc5bc20ab5'],
    'fashion-and-accessories': ['12465', '3', '7be6cfd9-93f2-4b46-a7bc-9561c963e78d'],
    'real-estate': ['12467', '5', 'ceb4000c-1071-4d71-8da5-13b0111d98f2'],
    'tv-audio-and-cameras': ['12545', '5', 'c3358436-6396-4904-b7e5-b67aed00cd1a'],
    'cell-phones': ['16000', '3', '57705fcd-cc62-4cee-a4a6-4cea84a416cb'],
    'electronics': ['15000', '5', '70500a4e-691a-40e4-8fec-380b69df172a'],
    'sports-leisure-and-games': ['12579', '11', 'aae73401-55c5-4fd5-943c-4c7a983aebd9'],
    'bikes': ['17000', '3', 'f22b1aca-d85d-465f-af1d-971730db4b0d'],
    'games-and-consoles': ['12900', '5', 'b5ccd554-8292-468b-965b-812062922d5a'],
    'home-and-garden': ['12467', '5', 'ceb4000c-1071-4d71-8da5-13b0111d98f2'],
    'appliances': ['13100', '3', '3ee725e2-5416-46ff-bb35-9ef757f83dd0'],
    'movies-books-music': ['12463', '5', '7936d0e6-0f98-459b-9603-74b1a4715549'],
    'baby-and-child': ['12461', '5', 'ea043270-23d2-4eea-8988-46e0742c82f2'],
    'collectibles-and-art': ['18000', '5', '6345ca72-ebe9-4ea1-88db-586de9411fe0'],
    'construction': ['19000', '3', 'c4a6bba9-293d-4a3c-abe8-87856baf432a'],
    'agriculture-industrial': ['20000', '3', '6aed6ed2-7149-4938-8d35-a654079129d1'],
    'jobs': ['21000', '5', '2ad66a50-46b5-4e3a-9af3-1b61ab5df73e'],
    'services': ['13200', '3', '526bf32d-bfde-4c93-9078-fb19b949a91d'],
    'other': ['12485', '5', 'f3cde3d8-63b0-4217-8bcb-4cf1f6668d01']
}

ctg_dict_reversed = {
    '100': 'cars',
    '14000': 'motorbikes',
    '12800': 'motors-and-accessories',
    '12465': 'fashion-and-accessories',
    '12545': 'tv-audio-and-cameras',
    '16000': 'cell-phones',
    '15000': 'electronics',
    '12579': 'sports-leisure-and-games',
    '17000': 'bikes',
    '12900': 'games-and-consoles',
    '12467': 'home-and-garden',
    '13100': 'appliances',
    '12463': 'movies-books-music',
    '12461': 'baby-and-child',
    '18000': 'collectibles-and-art',
    '19000': 'construction',
    '20000': 'agriculture-industrial',
    '21000': 'jobs',
    '13200': 'services',
    '12485': 'other'

}

month = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
         'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12', }
dict_city = {
    1: ['Leicester', 'East+Midlands'],
    2: ['Salford', 'North+West+England'],
    3: ['bristol', 'South+West+England'],
    4: ['birmingham', 'West+Midlands'],
    5: ['London', 'Greater+London']
}
dict_categories = {
    1: 'motors-and-accessories',
    2: 'fashion-and-accessories',
    3: 'tv-audio-and-cameras',
    4: 'cell-phones',
    5: 'sports-leisure-and-games',
    6: 'collectibles-and-art',
    7: 'home-and-garden',
    8: 'other',
    9: 'electronics'
}


def url_(url, protokol, start, step):
    if protokol == 'category':
        url = url + '&start=' + start + str(step)
    else:
        random_int = randint(1, 9)
        ctg_user = dict_categories[random_int]
        categories_id = ctg_dict[ctg_user][0]
        search_id = ctg_dict[ctg_user][2]
        step = ctg_dict[ctg_user][1]
        step = '&step=' + step
        random_int = randint(1, 5)
        city = dict_city[random_int][0]
        user_provinse = dict_city[random_int][1]
        url = f'https://api.wallapop.com/api/v3/general/search?user_province={user_provinse}&latitude=51.509865&user_region=England&user_city={city}&search_id={search_id}&country_code=GB&items_count=89&density_type=0&filters_source=quick_filters&order_by=newest&category_ids={categories_id}&longitude=-0.118092' + '&start=' + start + str(
            step)
    return url


async def start_parsing(message, conn):
    cur = conn.cursor()
    ID = message.from_user.id
    cur.execute('UPDATE stoped SET stop = %s WHERE id = %s', ('no', ID))
    conn.commit()

    cur.execute('SELECT url FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    URL_bd = cur.fetchone()[0]
    conn.commit()

    keyboard_stop = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_stop = ["Остановить парсинг"]
    keyboard_stop.add(*buttons_stop)
    await bot.send_message(ID, 'Процесс парсинга запущен. Это может занять некоторое время', reply_markup=keyboard_stop)

    if URL_bd.find('search') != -1 and URL_bd.find('category') == -1:
        protokol = 'all_categories'
        step = 'None'
        url = 'None'
    elif URL_bd.find('search') != -1 and URL_bd.find('category') != -1:
        categories_id = re.findall('category_ids=([0-9]+)&', URL_bd)[0]
        categories = ctg_dict_reversed[categories_id]
        categories_id = ctg_dict[categories][0]
        search_id = ctg_dict[categories][2]
        step = ctg_dict[categories][1]
        step = '&step=' + step

        random_int = randint(1, 5)
        city = dict_city[random_int][0]
        user_provinse = dict_city[random_int][1]
        url = f'https://api.wallapop.com/api/v3/general/search?user_province={user_provinse}&latitude=51.509865&user_region=England&user_city={city}&search_id={search_id}&country_code=GB&items_count=89&density_type=0&filters_source=quick_filters&order_by=newest&category_ids={categories_id}&longitude=-0.118092'
        protokol = 'category'
    else:
        ctg_user = re.sub('https://uk.wallapop.com/', '', URL_bd)
        categories_id = ctg_dict[ctg_user][0]
        search_id = ctg_dict[ctg_user][2]
        step = ctg_dict[ctg_user][1]
        step = '&step=' + step

        random_int = randint(1, 5)
        city = dict_city[random_int][0]
        user_provinse = dict_city[random_int][1]
        url = f'https://api.wallapop.com/api/v3/general/search?user_province={user_provinse}&latitude=51.509865&user_region=England&user_city={city}&search_id={search_id}&country_code=GB&items_count=89&density_type=0&filters_source=quick_filters&order_by=newest&category_ids={categories_id}&longitude=-0.118092'
        protokol = 'category'

    dabl_url = []
    for href in [url_(url, protokol, str(start * 40), step) for start in range(1, 250)]:
        print(href)
        ID = message.from_user.id
        cur.execute('SELECT stop FROM stoped WHERE id = %s LIMIT 1', (ID,))
        stop_bd = cur.fetchone()[0]
        if stop_bd == 'yes':
            break
        async with aiohttp.ClientSession() as session:
            HEADERS = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en,ru-RU;q=0.9,ru;q=0.8,en-US;q=0.7',
                'Authorization': 'Bearer ZLsxprGNdJCxwQhZ3b9Gn94v3ZiZacWI1FMND7GlBH6LLTfDaD5fN3QpzwcTgSKQlBgoZg',
                'Connection': 'keep-alive',
                'DeviceOS': '0',
                'Host': 'api.wallapop.com',
                'Origin': 'https://uk.wallapop.com',
                'Referer': 'https://uk.wallapop.com/',
                'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-site',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36',
                'X-DeviceOS': '0'
            }
            async with session.get(href, headers=HEADERS) as response:
                url_dict = await response.text()
        dabl_ur = await first_step(url_dict, str(message.from_user.id), conn, cur, dabl_url, protokol)
        dabl_url.extend(dabl_ur)
    await end(message, conn, cur, ID)


async def fetch(session, url):
    url = 'https://uk.wallapop.com/item/' + url
    HEADERS = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'
    }
    async with session.get(url, headers=HEADERS) as response:
        return await response.text()


async def first_step(url_dict, ID, conn, cur, dabl_url, protokol):
    cur.execute('SELECT opit FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    opit_bd = cur.fetchone()[0]
    cur.execute('SELECT data FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    DATA_bd = cur.fetchone()[0]
    items_list = re.findall('"web_slug":"(.+?)",', url_dict)
    cur.execute('SELECT proxy_list FROM proxy WHERE id = %s', ('123123',))
    proxy_list = cur.fetchall()[0][0]
    proxy_list = ast.literal_eval(proxy_list)
    proxy = random.choice(proxy_list)
    connector = ProxyConnector.from_url(proxy)
    loop = asyncio.get_event_loop()
    async with aiohttp.ClientSession(loop=loop) as session:
        html_links = await asyncio.gather(*[fetch(session, url) for url in items_list])

    dabl_url = []
    for item in html_links:
        print('')
        cur.execute('SELECT all_count FROM num WHERE id = %s LIMIT 1', (ID,))
        all_count_ = int(cur.fetchone()[0]) + 1
        cur.execute('UPDATE num SET all_count = %s WHERE id = %s', (all_count_, ID))
        conn.commit()

        soup = BeautifulSoup(item, 'lxml')
        try:
            data = soup.find('div', class_="card-product-detail-user-stats-published").get_text(strip=True)
        except:
            print('data error')
            continue
        if DATA_bd == 'No':
            data_re = 'No'
        else:
            day = re.findall('([0-9]{2})-.+', data)[0]
            mon = re.findall('[0-9]{2}-(.{3})-.+', data)[0]
            mon = month[mon]
            year = re.findall('.+-([0-9]{4})', data)[0]
            data_re = day + '.' + mon + '.' + year
        if DATA_bd == data_re:
            how_many_tovar = soup.find('div', class_="card-user-detail-rating").get_text(strip=True)
            how_many_tovar = re.sub("[^0-9]", "", how_many_tovar)
            if int(how_many_tovar) <= int(opit_bd):
                cur.execute('SELECT stop FROM stoped WHERE id = %s LIMIT 1', (ID,))
                stop_bd = cur.fetchone()[0]
                if stop_bd == 'yes':
                    break
                cur.execute('SELECT success FROM num WHERE id = %s LIMIT 1', (ID,))
                success_ = int(cur.fetchone()[0]) + 1
                cur.execute('UPDATE num SET success = %s WHERE id = %s', (success_, ID))
                conn.commit()

                link = soup.find('link', rel="canonical").get('href')
                if link in dabl_url:
                    continue
                else:
                    dabl_url.append(link)
                product_img = soup.find('meta', property="og:image").get('content')
                title = soup.find('h1', id="item-detail-title").get_text(strip=True)
                price = soup.find('span', class_="bold card-user-detail-item-price").get_text(strip=True)
                chat_url = 'https://uk.wallapop.com/app/chat?itemId=' + soup.find('div', class_="detail-item").get(
                    'data-item-uuid')

                message_text = (
                            '<code>' + str(title) + '</code>' + '\nссылка: ' + str(link) + '\nцена: ' + '<code>' + str(
                        price) + '</code>' + '\nкартинка: ' + str(
                        product_img) + '\nколичество сделок у продавца: ' + str(
                        how_many_tovar) + '\nдата размещения объявления: ' + str(data))
                buttons = [
                    types.InlineKeyboardButton(text='Написать продавцу', url=chat_url)
                ]
                keyboard = types.InlineKeyboardMarkup()
                keyboard.add(*buttons)
                await bot.send_message(ID, message_text, parse_mode="HTML", reply_markup=keyboard)
                time.sleep(0.5)
            else:
                cur.execute('SELECT error3 FROM num WHERE id = %s LIMIT 1', (ID,))
                error3_ = int(cur.fetchone()[0]) + 1
                cur.execute('UPDATE num SET error3 = %s WHERE id = %s', (error3_, ID))
                conn.commit()
        else:
            cur.execute('SELECT error2 FROM num WHERE id = %s LIMIT 1', (ID,))
            error2_ = int(cur.fetchone()[0]) + 1
            cur.execute('UPDATE num SET error2 = %s WHERE id = %s', (error2_, ID))
            conn.commit()
    return dabl_url


async def end(message, conn, cur, ID):
    cur.execute('SELECT success FROM num WHERE id = %s LIMIT 1', (ID,))
    success_ = cur.fetchone()[0]
    cur.execute('SELECT error2 FROM num WHERE id = %s LIMIT 1', (ID,))
    error2_ = cur.fetchone()[0]
    cur.execute('SELECT error3 FROM num WHERE id = %s LIMIT 1', (ID,))
    error3_ = cur.fetchone()[0]
    cur.execute('SELECT all_count FROM num WHERE id = %s LIMIT 1', (ID,))
    all_count_ = cur.fetchone()[0]
    end_sey = 'парсинг завершен \n' + \
              'всего спарсили: ' + str(all_count_) + ' объявлений\n' + \
              'подошло ' + str(success_) + ' обьявлений\n' + \
              'не подходит по дате размещения ' + str(error2_) + ' обьявлений\n' + \
              'сделок слишком много у ' + str(error3_) + ' обьявлений\n'
    await bot.send_message(message.from_user.id, end_sey, reply_markup=keyboard.menu())
