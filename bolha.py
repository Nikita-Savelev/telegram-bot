from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram import Bot, Dispatcher, types
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import re
import psycopg2
import asyncio
import aiohttp
import ssl
import random
import ast
import config
import keyboard

bot = Bot(config.token_bot)
dp = Dispatcher(bot, storage=MemoryStorage())

HOST = 'https://www.bolha.com'
ua = UserAgent()
HEADERS = {'User-Agent': ua.chrome}


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
    conn.commit()

    keyboard_stop = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    buttons_stop = ["Остановить парсинг"]
    keyboard_stop.add(*buttons_stop)
    await bot.send_message(ID, 'Процесс парсинга запущен. Это может занять некоторое время', reply_markup=keyboard_stop)

    dabl_url = []
    for page in range(PAGENATION_ot_bd, PAGENATION_do_bd):
        ID = message.from_user.id
        cur.execute('SELECT stop FROM stoped WHERE id = %s LIMIT 1', (ID,))
        stop_bd = cur.fetchone()[0]
        if stop_bd == 'yes':
            pages_print = 'Парсинг остановлен на странице ' + str(page)
            await bot.send_message(message.from_user.id, pages_print)
            break
        url = URL_bd + '?page=' + str(page)
        # pages_print='парсим страницу: '+str(page)
        # await bot.send_message(message.from_user.id, pages_print)
        print(page)
        dabl_url = await first_step(url, str(message.from_user.id), conn, cur, dabl_url)
    await end(message, cur, ID)


async def first_step(url, ID, conn, cur, dabl_url):
    cur.execute('SELECT proxy_list FROM proxy WHERE id = %s', ('123123',))
    proxy_list = cur.fetchall()[0][0]
    proxy_list = ast.literal_eval(proxy_list)
    proxy = random.choice(proxy_list)
    path_url = re.sub('https://www.bolha.com', '', url)
    HEADER = {
        'authority': 'www.bolha.com',
        'method': 'GET',
        'path': path_url,
        'scheme': 'https',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'cookie': 'DM_SitId475=true; DM_SitId475SecId2068=true; __utmc=62098995; _fbp=fb.1.1637415174660.506374525; __gads=ID=842594b533b16681:T=1637415175:S=ALNI_MZLJ8hp862vtopbg3I6TpsTZodjfw; _hjSessionUser_1864642=eyJpZCI6ImY0ZjI1YmEzLTc0MmMtNTc5Zi1iMThmLWZhNTVjZWZkYTg2MyIsImNyZWF0ZWQiOjE2Mzc0MTUxNzQ5MDMsImV4aXN0aW5nIjp0cnVlfQ==; DM_SitId475SecId2097=true; DM_SitId475SecId2092=true; DM_SitId475SecId2124=true; DM_SitId475SecId2099=true; DM_SitId475SecId2081=true; DM_SitId475SecId2090=true; didomi_token=eyJ1c2VyX2lkIjoiMTdkM2Q4YzktYzJlOC02OTcwLTg4Y2EtNzQ1ODU0YzA5ZjdkIiwiY3JlYXRlZCI6IjIwMjEtMTEtMjRUMTM6NDM6MzUuMDA0WiIsInVwZGF0ZWQiOiIyMDIxLTExLTI0VDEzOjQzOjM1LjAwNFoiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwiYzpwaW50ZXJlc3QiLCJjOmhvdGphciIsImM6bmV3LXJlbGljIiwiYzpib29raXQtOWZobmI0RmgiLCJjOmRvdG1ldHJpYy0zUW1oYnRYayIsImM6ZG90bWV0cmljLWRIQnFrWEtlIiwiYzpkb3RtZXRyaWMtWGNyTnhaSEsiLCJjOmV0YXJnZXQtWmluOHBiek0iLCJjOmlzbG9ubGluZS1BM0ZDanBYZCIsImM6c3R5cmlhLWFkVFc0TU5hIiwiYzp4aXRpLUN6Tjd6QTR5IiwiYzpkb3VibGVjbGljLVllbVJWckNSIiwiYzpnb29nbGVhbmEtNFRYbkppZ1IiLCJjOmdvb2dsZWFkcy04SEI2blRWQiJdfSwicHVycG9zZXMiOnsiZW5hYmxlZCI6WyJkZXZpY2VfY2hhcmFjdGVyaXN0aWNzIiwiZ2VvbG9jYXRpb25fZGF0YSJdfSwidmVuZG9yc19saSI6eyJlbmFibGVkIjpbImdvb2dsZSJdfSwidmVyc2lvbiI6MiwiYWMiOiJBa3VBRUFGa0JKWUEuQWt1QUNBa3MifQ==; b_tp={"ga":"true","gads":"true","fb":"true"}; DM_SitId475Stress=true; DM_SitId475SecId2092Stress=true; DM_SitId475SecId2124Stress=true; DM_SitId475SecId2072=true; DM_SitId475SecId2084=true; DM_SitId475SecId4267=true; DM_SitId475SecId2078=true; DM_SitId475SecId2068Stress=true; euconsent-v2=CPQJwUAPQJwUAAHABBENCECsAP_AAH_AAAAAIXtf_X__b3_j-_59f_t0eY1P9_7_v-0zjhfdt-8N2f_X_L8X42M7vF36pq4KuR4Eu3LBIQdlHOHcTUmw6okVrzPsbk2cr7NKJ7PEmnMbO2dYGH9_n93TuZKY7__8___z_v-v_v____f_7-3_3__5_X---_e_V399zLv9____39nP___9v-_9-CF4BJhqXkAXYljgybRpVCiBGFYSHQCgAooBhaIrCBlcFOyuAj1BCwAQmoCMCIEGIKMGAQACAQBIREBIAeCARAEQCAAEAKkBCAAjYBBYAWBgEAAoBoWIEUAQgSEGRwVHKYEBEi0UE9lYAlB3saYQhllgBQKP6KhARKEECwMhIWDmOAJAS4WSBZgAAAAA.f_gAD_gAAAAA; DM_SitId475SecId2077=true; DM_SitId475SecId2077Stress=true; __utmz=62098995.1648459499.3.2.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); _gcl_au=1.1.568248660.1648463054; _ga=GA1.1.587309559.1637415165; bolha_adblock_detected=true; DM_SitId475SecId2078Stress=true; DM_SitId475SecId2072Stress=true; PHPSESSID=e82e3533e05cde37d456552e35821798; DM_SitId475SecId2091=true; DM_SitId475SecId2091Stress=true; DM_SitId475SecId2090Stress=true; DM_SitId475SecId2079=true; DM_SitId475SecId2079Stress=true; DM_SitId475SecIdT2124=true; DM_SitIdT475=true; _hjSession_1864642=eyJpZCI6IjE3OWUyY2IxLWNkZjUtNGUwNS05NTM3LWU4MjJlYTNiNTU1OCIsImNyZWF0ZWQiOjE2NDg1NzM2NjgwMTEsImluU2FtcGxlIjp0cnVlfQ==; _hjIncludedInSessionSample=1; _hjAbsoluteSessionInProgress=0; DM_SitId475SecIdT2124Stress=true; DM_SitIdT475Stress=true; __utma=62098995.587309559.1637415165.1648566776.1648573691.15; __utmt=1; __utmb=62098995.1.10.1648573691; _ga_H9M3NJEC3L=GS1.1.1648573664.14.1.1648573690.0; DM_SitId475SecIdT2092=true; _ga_1GCTTPHB0C=GS1.1.1648573664.14.1.1648573691.0; udf=1; DM_SitId475SecIdT2092Stress=true',
        'referer': 'https://www.bolha.com/telefonija',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36',
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADER) as resp:
            html = await resp.text()
    items_list = []
    soup = BeautifulSoup(html, 'lxml')
    items = soup.find_all('article', class_="entity-body cf")
    count = 0
    for item in items:
        count += 1
        if count == 26:
            break
        cur.execute('SELECT all_count FROM num WHERE id = %s LIMIT 1', (ID,))
        all_count_ = int(cur.fetchone()[0]) + 1
        cur.execute('UPDATE num SET all_count = %s WHERE id = %s', (all_count_, ID))
        conn.commit()
        links = HOST + item.find('a', class_="link").get('href')
        items_list.append(links)
    dabl_url = await second_step(items_list, ID, session, conn, cur, dabl_url)
    return dabl_url


async def fetch(session, url):
    HEADERS = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'
    }
    timeout = aiohttp.ClientTimeout(total=5)
    rool = 'true'
    while rool == 'true':
        try:
            async with session.get(url, headers=HEADERS, timeout=timeout) as response:
                return await response.text()
        except:
            pass


async def fetch_account(session, url):
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'
    }
    async with session.get(url, ssl=ssl.SSLContext(), headers=headers) as resp:
        try:
            return await resp.text()
        except:
            print('utf-8 encoding_error')
            return 'none'


async def second_step(items_list, ID, session, conn, cur, dabl_url):
    loop = asyncio.get_event_loop()
    async with aiohttp.ClientSession(loop=loop) as session_loop:
        cur.execute('SELECT data FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
        DATA_bd = cur.fetchone()[0]
        html_links = await asyncio.gather(*[fetch(session_loop, url) for url in items_list])
        items_dict1 = {}
        items_list1 = []
        for item in html_links:
            soup = BeautifulSoup(item, 'lxml')
            f = open('soup_2212.txt', 'w+', encoding='utf-8')
            f.write(str(soup))
            f.close()
            try:
                data = soup.find('time', class_="value").get_text(strip=True)
                data_re_sus = re.findall('(.+) dne', data)
            except:
                try:
                    data = soup.find('dd', class_="ClassifiedDetailSystemDetails-listData").get_text(strip=True)
                    data_re_sus = re.findall('(.+)\.\s.+', data)
                except:
                    data = soup.find('time', class_="date date--full").get_text(strip=True)
                    data_re_sus = re.findall('(.+)\.', data)
            if DATA_bd == 'No':
                data_re = 'No'
            else:
                data_re = data_re_sus
            if type(data_re) is list:
                data_re = data_re[0]
            if DATA_bd == data_re:
                try:
                    phone = soup.find('a', class_="link link-tel link-tel--alpha").get_text(strip=True)
                    phon = re.findall('Telefonska številka:\s+([+0-9 ].+)', phone)[0]
                    phone = re.sub("[^0-9]", "", phone)
                except:
                    try:
                        phone = soup.find('a',
                                          class_="ClassifiedDetailOwnerDetails-contactEntryLink link-tel link-tel--gamma link-tel--faux").get(
                            'data-display')
                        phone = re.sub("[^0-9]", "", phone)
                    except:
                        phone = 'None'
                if phone != 'None':
                    try:
                        link = soup.find('a', class_="Profile-username link").get('href')
                    except:
                        try:
                            link = soup.find('a', class_="link-standard").get('href')
                        except:
                            continue
                    try:
                        name = soup.find('a', class_="Profile-username link").get_text(strip=True).lower()
                    except:
                        name = soup.find('a', class_="link-standard").get_text(strip=True).lower()
                    links = soup.find('link', rel="canonical").get('href')
                    try:
                        product_img = soup.find('img', class_="ArrangeFit-content FlexImage-content").get('src')
                        product_img = 'https:' + product_img
                    except:
                        try:
                            product_img = soup.find('meta', property="og:image").get('content')
                        except:
                            product_img = 'None'
                    try:
                        title = soup.find('h1', class_="entity-title").get_text()
                        price = soup.find('strong', class_="price price--hrk").get_text(strip=True)
                    except:
                        title = soup.find('h1', class_="ClassifiedDetailSummary-title").get_text()
                        price = soup.find('dd', class_="ClassifiedDetailSummary-priceDomestic").get_text(strip=True)
                    item_dict = {name: {
                        'link': links,
                        'img': product_img,
                        'title': title,
                        'price': price,
                        'data': data_re_sus[0],
                        'phone': phone
                    }}
                    items_dict1.update(item_dict)
                    items_list1.append(link)
                else:
                    cur.execute('SELECT error1 FROM num WHERE id = %s LIMIT 1', (ID,))
                    error1_ = int(cur.fetchone()[0]) + 1
                    cur.execute('UPDATE num SET error1 = %s WHERE id = %s', (error1_, ID))
                    conn.commit()
            else:
                cur.execute('SELECT error2 FROM num WHERE id = %s LIMIT 1', (ID,))
                error2_ = int(cur.fetchone()[0]) + 1
                cur.execute('UPDATE num SET error2 = %s WHERE id = %s', (error2_, ID))
                conn.commit()
        print(f'second_step compleat at {ID}')
        dabl_url = await third_step(items_list1, items_dict1, ID, session_loop, conn, cur, dabl_url)
        return dabl_url


async def third_step(items_list1, items_dict1, ID, session_loop, conn, cur, dabl_url):
    cur.execute('SELECT opit FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    opit_bd = cur.fetchone()[0]
    cur.execute('SELECT _text_ FROM wtext WHERE id = %s LIMIT 1', (ID,))
    url_text = cur.fetchone()[0]
    if url_text is None:
        url_text = 'pozdravljeni%2C+je+ta+oglas+%C5%A1e+aktiven%3F'
    print(url_text)
    html_links = await asyncio.gather(*[fetch_account(session_loop, url) for url in items_list1])
    for item in html_links:
        if item == 'none':
            continue
        soup = BeautifulSoup(item, 'lxml')
        try:
            how_many = soup.find('strong', class_="entities-count").get_text(strip=True)
            how_many_tovar = int(how_many.strip())
        except:
            continue
        name = soup.find('h1', class_="UserProfileDetails-title").get_text(strip=True).lower()
        link = name
        if opit_bd >= how_many_tovar:
            cur.execute('SELECT success FROM num WHERE id = %s LIMIT 1', (ID,))
            success_ = int(cur.fetchone()[0]) + 1
            cur.execute('UPDATE num SET success = %s WHERE id = %s', (success_, ID))
            conn.commit()
            if dabl_url.count(str(items_dict1[link]['link'])) > 0:
                continue
            else:
                dabl_url.append(str(items_dict1[link]['link']))
            message_text = ('<code>' + str(items_dict1[link]['title']) + '</code>' + '\nссылка: ' + str(
                items_dict1[link]['link']) + '\nцена: ' + '<code>' + str(
                items_dict1[link]['price']) + '</code>' + '\nкартинка: ' + str(
                items_dict1[link]['img']) + '\nколичество сделок у продавца: ' + str(
                how_many_tovar) + '\nдата размещения объявления: ' + str(
                items_dict1[link]['data']) + '\nтелефон: ' + '<code>386' + str(items_dict1[link]['phone']) + '</code>')
            buttons = [
                types.InlineKeyboardButton(text='Написать в Whatsap',
                                           url='https://wa.me/{0}?text={1}+{2}'.format(str(items_dict1[link]['phone']),
                                                                                       url_text,
                                                                                       items_dict1[link]['link']))
            ]
            keyboard = types.InlineKeyboardMarkup()
            keyboard.add(*buttons)
            await bot.send_message(ID, message_text, parse_mode="HTML", reply_markup=keyboard)
        else:
            cur.execute('SELECT error3 FROM num WHERE id = %s LIMIT 1', (ID,))
            error3_ = int(cur.fetchone()[0]) + 1
            cur.execute('UPDATE num SET error3 = %s WHERE id = %s', (error3_, ID))
            conn.commit()
    print(f'third_step compleat at {ID}')
    return dabl_url


async def end(message, cur, ID):
    cur.execute('SELECT success FROM num WHERE id = %s LIMIT 1', (ID,))
    success_ = cur.fetchone()[0]
    cur.execute('SELECT error1 FROM num WHERE id = %s LIMIT 1', (ID,))
    error1_ = cur.fetchone()[0]
    cur.execute('SELECT error2 FROM num WHERE id = %s LIMIT 1', (ID,))
    error2_ = cur.fetchone()[0]
    cur.execute('SELECT error3 FROM num WHERE id = %s LIMIT 1', (ID,))
    error3_ = cur.fetchone()[0]
    cur.execute('SELECT error4 FROM num WHERE id = %s LIMIT 1', (ID,))
    error4_ = cur.fetchone()[0]
    cur.execute('SELECT all_count FROM num WHERE id = %s LIMIT 1', (ID,))
    all_count_ = cur.fetchone()[0]
    end_sey = 'парсинг завершен \n' + \
              'всего спарсили: ' + str(all_count_) + ' объявлений\n' + \
              'подошло ' + str(success_) + ' обьявлений\n' + \
              'номер отсутствует у ' + str(error1_) + ' обьявлений\n' + \
              'не подходит по дате размещения ' + str(error2_) + ' обьявлений\n' + \
              'сделок слишком много у ' + str(error3_) + ' обьявлений\n' + \
              'комерческие предложения ' + str(error4_)
    await bot.send_message(message.from_user.id, end_sey, reply_markup=keyboard.menu())
