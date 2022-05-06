from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram import Bot, Dispatcher, types
import time
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import re
# import sqlite3
import psycopg2
import asyncio
import aiohttp
from aiohttp_proxy import ProxyConnector
import random
from random import randint
import ast
import config
import keyboard

bot = Bot(config.token_bot)
dp = Dispatcher(bot, storage=MemoryStorage())

HOST = 'https://it.wallapop.com'
HOST_ITEM = 'https://it.wallapop.com/item/'
ua = UserAgent()
HEADERS = {'User-Agent': ua.chrome}
ctg_dict = {
    'auto': ['14000', '5', '051253ff-b3ba-49a7-97dd-70e81d157f24'],
    'moto': ['14000', '5', '2155c03f-2ad5-491a-ae46-c12969f411b7'],
    'motori-e-accessori': ['12800', '2', 'f6370a19-7445-45b3-9ffc-c11786e9cf33'],
    'moda-e-accessori': ['12465', '3', '0d61dd20-81a9-4472-9a25-a77814a2e0f3'],
    'immobiliare': ['12467', '5', '428fdfd6-0f01-4153-9702-973bd59146cd'],
    'tv-audio-e-fotocamere': ['12545', '5', '9170003b-65e9-49d4-b33d-e65fbe4f0e3a'],
    'telefonia-e-accessori': ['16000', '5', '6d18334f-5b6d-4a6f-9315-cc117ce2d863'],
    'informatica-e-elettronica': ['15000', '5', 'afed84a0-b84c-42cf-a565-3908970e34df'],
    'sport-e-tempo-libero': ['12579', '10', '6685fc89-5074-4a1a-af44-3365a3d28fc8'],
    'biciclette': ['17000', '5', '9a32b001-8c06-464c-913c-21248d430a02'],
    'console-e-videogiochi': ['12900', '5', '4c0478cb-dd99-4a01-9fd6-33272297d00f'],
    'casa-e-giardino': ['12467', '5', '428fdfd6-0f01-4153-9702-973bd59146cd'],
    'elettrodomestici': ['13100', '5', '46e6aa26-89ef-4265-94c6-91efa91d9fdb'],
    'cinema-libri-musica': ['12463', '5', 'f03138c7-63cf-4546-9d87-93c82b77e3ae'],
    'bambini-e-neonati': ['12461', '5', 'd4f9c361-cc99-41b0-9222-87afbec52588'],
    'collezionismo': ['18000', '5', '87c7a8ef-5abf-4dc4-8dc0-0a0a294ee6df'],
    'attrezzature-di-lavoro': ['19000', '5', '77e682fb-a3b1-431e-b702-0e572ffab847'],
    'industria-e-agricoltura': ['20000', '5', '7916e1aa-e47d-454c-9c31-504371eb615d'],
    'lavoro': ['13200', '4', '0f13a6a9-ba2a-4f5a-b9eb-ac45c045df75'],
    'servizi': ['13200', '4', '0f13a6a9-ba2a-4f5a-b9eb-ac45c045df75'],
    'altro': ['12485', '5', 'cf60de2d-adb0-45e2-aa55-0d8cab306799']
}

ctg_dict_reversed = {
    '100': 'auto',
    '14000': 'moto',
    '12800': 'motori-e-accessori',
    '12465': 'moda-e-accessori',
    '12467': 'immobiliare',
    '12545': 'tv-audio-e-fotocamere',
    '16000': 'telefonia-e-accessori',
    '15000': 'informatica-e-elettronica',
    '12579': 'sport-e-tempo-libero',
    '17000': 'biciclette',
    '12900': 'console-e-videogiochi',
    '13100': 'elettrodomestici',
    '12463': 'cinema-libri-musica',
    '12461': 'bambini-e-neonati',
    '18000': 'collezionismo',
    '19000': 'attrezzature-di-lavoro',
    '20000': 'industria-e-agricoltura',
    '21000': 'lavoro',
    '13200': 'servizi',
    '12485': 'altro'

}

month = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
         'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12', }
dict_categories = {
    1: 'motori-e-accessori',
    2: 'moda-e-accessori',
    3: 'tv-audio-e-fotocamere',
    4: 'telefonia-e-accessori',
    5: 'sport-e-tempo-libero',
    6: 'industria-e-agricoltura',
    7: 'cinema-libri-musica',
    8: 'altro',
    9: 'informatica-e-elettronica'
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
        url = f'https://api.wallapop.com/api/v3/general/search?user_province=Roma&latitude&latitude=41.8905&user_region=England&user_city=Roma&search_id={search_id}&country_code=IT&items_count=89&density_type=0&filters_source=quick_filters&order_by=newest&category_ids={categories_id}&longitude=12.4942' + '&start=' + start + str(
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
        url = f'https://api.wallapop.com/api/v3/general/search?user_province=Roma&latitude&latitude=41.8905&user_region=England&user_city=Roma&search_id={search_id}&country_code=IT&items_count=89&density_type=0&filters_source=quick_filters&order_by=newest&category_ids={categories_id}&longitude=12.4942'
        protokol = 'category'
    else:
        ctg_user = re.sub('https://it.wallapop.com/', '', URL_bd)
        categories_id = ctg_dict[ctg_user][0]
        search_id = ctg_dict[ctg_user][2]
        step = ctg_dict[ctg_user][1]
        step = '&step=' + step
        url = f'https://api.wallapop.com/api/v3/general/search?user_province=Roma&latitude&latitude=41.8905&user_region=England&user_city=Roma&search_id={search_id}&country_code=IT&items_count=89&density_type=0&filters_source=quick_filters&order_by=newest&category_ids={categories_id}&longitude=12.4942'
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
                'Accept-Language': 'it,ru-RU;q=0.9,ru;q=0.8,en-US;q=0.7,en;q=0.6',
                'Authorization': 'Bearer ZLsxprGNdJCxwQhZ3b9Gn94v3ZiZacWI1FMND7GlBH6LLTfDaD5fN3QpzwcTgSKQlBgoZg',
                'Connection': 'keep-alive',
                'DeviceOS': '0',
                'Host': 'api.wallapop.com',
                'Origin': 'https://it.wallapop.com',
                'Referer': 'https://it.wallapop.com/',
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
            f = open('wallapop.txt', 'w+', encoding='utf-8')
            f.write(str(url_dict))
            f.close()
        dabl_ur = await first_step(url_dict, str(message.from_user.id), conn, cur, dabl_url, protokol)
        dabl_url.extend(dabl_ur)
    await end(message, conn, cur, ID)


async def fetch(session, url):
    url = 'https://it.wallapop.com/item/' + url
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
                chat_url = 'https://it.wallapop.com/app/chat?itemId=' + soup.find('div', class_="detail-item").get(
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
