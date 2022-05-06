from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram import Bot, Dispatcher
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import requests
import re
import psycopg2
import asyncio
import aiohttp
from aiohttp_proxy import ProxyConnector
import ssl
import random
import ast
import config
import keyboard

bot = Bot(config.token_bot)
dp = Dispatcher(bot, storage=MemoryStorage())

HOST = 'http://gumtree.com'

ua = UserAgent()
HEADERS = {'User-Agent': ua.chrome}

cookies = [{'domain': '.my.gumtree.com', 'expiry': 1644790713, 'httpOnly': False, 'name': 'gt_mc', 'path': '/',
            'secure': False, 'value': '"rcd:MTY0NDcwNDczMzM1Nw==|nuc:MA=="'},
           {'domain': 'my.gumtree.com', 'expiry': 1644790713, 'httpOnly': True, 'name': 'NSC_tfmmfs-iuuq', 'path': '/',
            'secure': True, 'value': 'ffffffff092f8e2945525d5f4f58455e445a4a4229a0'},
           {'domain': '.gumtree.com', 'expiry': 1802384313, 'httpOnly': False, 'name': 'gt_userPref', 'path': '/',
            'secure': True,
            'value': 'isSearchOpen:dHJ1ZQ==|recentAdsOne:Y2Fycy12YW5zLW1vdG9yYmlrZXM=|cookiePolicy:dHJ1ZQ==|recentAdsTwo:Zm9yLXNhbGU=|location:dWs='},
           {'domain': '.gumtree.com', 'expiry': 1802384312, 'httpOnly': False, 'name': 'gt_mc', 'path': '/',
            'secure': True, 'value': 'rcd:MA==|nuc:MA=='},
           {'domain': '.gumtree.com', 'expiry': 1676240312, 'httpOnly': True, 'name': 'gt_rememberMe', 'path': '/',
            'secure': True,
            'value': 'hlHV87rpkciiX7XiNAgAGOM+4P5+e/o2w1vU29DiOGjUvVYcrouX4IE4+SkltI74XVVNgImG0XhywvZk21l2HqNMCbkWlZ7nzUUJ3jln7WlkURZRBhq+38y2MecYI+OW8sx8Vv9LE2gE/mKRm2Kq7I469y/m9JwSc9cyLtpMYhZoC16Ut+yNWTbtRgVG77Ui'},
           {'domain': '.gumtree.com', 'expiry': 1676240311, 'httpOnly': False, 'name': 'eupubconsent-v2', 'path': '/',
            'sameSite': 'Lax', 'secure': False,
            'value': 'CPUUfX5PUUfX5AcABBENCCCsAP_AAAAAAAYgIGNf_X__b3_j-_59f_t0eY1P9_7_v-0zjhfdt-8N2f_X_L8X_2M7vF36pq4KuR4Eu3LBIQVlHOHcTUmw6okVrTPsbk2Mr7NKJ7PEmnMbO2dYGH9_n93T-ZKY7______7_v-_______f__-_____5_3---_f_V_99zLv9__3__9wAAAPAAAAkEgogAIAAXABQAFQAMgAcAA8ACAAGEANAA1AB5AEMARQAmABPACqAFgAN4AcwA9ACEAENAIgAiYBLAEuAJoAUoAtwBhgDIAGqANkAd4A9gB8QD7AP0AgEBFwEYAI0ARwAlIBQQCngFXALmAYoA1gBtADcAHEAPQAhsBDoCRAExAJlATYAnYBQ4CkQFigLYAXIAu8BeYDBgGEgMNAYeAyIBkgDJwGXAM5AZ8A0gBp0DWANZAbrA5EDlQHLgOsAeOA-UIAOAHMAYQBT4DJgHSAOwAdmA7oB4ADygHtAPdAfIA-wNAcAC4AIYAZAA2QB-AEAAIwAU8Aq8BaAFpANYAh0BIgCbAE7AKRAXIAwkBh4DGAGTgM5AZ4Az4ByQDlAHWAPwDAAwBzAOzAe6IACAA1AHMA7MB7oiAwAIYAZAA2QB-AEAAIwAU8Aq4BrAEOgJEATYAnYBSIC5AGEgMPAZOAzkBnwDkgHKAOsAfgKgMAAUACGAEwALgA_ACMAEcAKvAWgBaQEggJiATYApsBbAC5AF5gMPAZEAzkBngDPgHJAOUAfgKABgDmAHgA-wZAWAAoAEMAJkA-wD8AIwARwAq4BWwExAJsAWiAtgBeYDDwGRAM5AZ4Az4ByQDlAHxAPwGABAAagDmAHgA-wdBrAAXABQAFQAMgAcABAAC6AGAAYwA0ADUAHgAPoAhgCKAEwAJ4AVQAsABcAC-AGIAMwAbwA5gB6AENAIgAiYBLAEwAJoAUYApQBYgC3gGEAYYAyABlADRAGyAN8Ad4A9oB9gH6AP-AiwCMAEcgJSAlQBQQCngFXALFAWgBaQC5gF5AMUAbQA3ABxADpgHoAQ2Ah0BEQCLwEggJEASoAmwBOwChwFNAKsAWLAtgC2QFwALkAXaAu8BeYDBgGEgMNAYeAxIBjADHgGSAMnAZUAywBlwDOQGfANEgaQBpIDSwGnANVAawA2MBuoDi4HJAcqA5cB1gDxwHpAPVAfKA-sB-A4AcAOYAwgDJgG2AOQAdIA7AB2YDwAHlAPaAe6A-IB9hCB2AAsACgAGQAXAAxACGAEwAKoAXAAvgBiADMAG8APQAsQBhADfAHfAPsA_AB_gEYAI4ASkAoIBQwCngFXgLQAtIBcwDFAG0APQAkEBIgCVAE2AKaAWKAtGBbAFtALgAXIAu0Bh4DEgGRAMnAZyAzwBnwDRAGkgNLAaqA4AByQDrAHjgPwIACgBzADwAMIA2wB2ADygHogPdAfEA-wlAzAAQAAsACgAGQAOQAwADEAHgARAAmABVAC4AF8AMQAZgBDQCIAIkAUYApQBbgDCAGqANkAd4A_ACMAEcAKeAVeAtAC0gGKANwAh0BF4CRAE2ALFAWwAu0BeYDDwGRAMnAZYAzkBngDPgGkANYAcAA6wB-BIAMAOYB0gDsAHlAPaAfYUgmAALgAoACoAGQAOAAggBgAGMANAA1AB5AEMARQAmABPACkAFUALAAXwAxABmADmAIaARABEgCjAFKALEAW4AwgBlADRAGqANkAd8A-wD9AIsARgAjgBKQCggFDAKuAVsAuYBeQDaAG4APQAh0BF4CRAE2AJ2AUOAsUBbAC4AFyALtAXmAw0Bh4DGAGRAMkAZOAy4BnIDPAGfQNIA0mBrAGsgNjAbrA5MDlAHLgOsAeOA-UB-BQAYAOYAeABhAFPgMmAdgA7MB5QD2gHugPiAfYAAA.f_gAAAAAAAAA'},
           {'domain': '.gumtree.com', 'expiry': 1676240311, 'httpOnly': False, 'name': 'OptanonAlertBoxClosed',
            'path': '/', 'sameSite': 'Lax', 'secure': False, 'value': '2022-02-12T22:18:31.776Z'},
           {'domain': '.my.gumtree.com', 'httpOnly': True, 'name': 'rbzid', 'path': '/', 'secure': False,
            'value': '+aUvxJ3dur4dTayX21tKz8LeyIORySaWq0UZ2UbQ/+GH5Z80F3IZRTiQX37BNHU/BTSztzSqmbG+sw16FRJ5odqAjJS2EKPMWDM9obNnWVoYo25s/Z2Z1CgeqdBrZwobV5vc+YEFVFi/yHnm0Ab3Mtu/YNAkPfAEZKHGm6/JH6TtMcBTk5fHCLYYmvnIu0QlbAjv9mEVBPOESC8VeHspnCdylrLkxcK47mcj1jihatqMLxjI6moN077bTkGMeiGuhPdmiS64DS6yaR/w1laKLpbojSnG5/4MReSFA3W/R9w='},
           {'domain': '.gumtree.com', 'expiry': 1676240313, 'httpOnly': False, 'name': 'OptanonConsent', 'path': '/',
            'sameSite': 'Lax', 'secure': False,
            'value': 'isIABGlobal=false&datestamp=Sun+Feb+13+2022+01%3A18%3A33+GMT%2B0300+(%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0%2C+%D1%81%D1%82%D0%B0%D0%BD%D0%B4%D0%B0%D1%80%D1%82%D0%BD%D0%BE%D0%B5+%D0%B2%D1%80%D0%B5%D0%BC%D1%8F)&version=6.10.0&hosts=H107%3A1%2CH257%3A1%2CH231%3A1%2CH233%3A1%2CH256%3A1%2CH600%3A1&consentId=f73df244-c869-420c-8c9a-cc8e583fcac3&interactionCount=1&landingPath=NotLandingPage&groups=FACEB%3A1%2CLIVER%3A1%2CSTACK42%3A1%2CC0026%3A1%2CC0028%3A1%2CC0029%3A1%2CC0023%3A1%2CGAPTS%3A1&geolocation=GB%3BENG'},
           {'domain': '.my.gumtree.com', 'expiry': 1644704365, 'httpOnly': False, 'name': '_gat', 'path': '/',
            'secure': False, 'value': '1'},
           {'domain': 'my.gumtree.com', 'expiry': 1644706113, 'httpOnly': False, 'name': 'lux_uid', 'path': '/',
            'sameSite': 'Lax', 'secure': False, 'value': '164470430419580826'},
           {'domain': '.my.gumtree.com', 'expiry': 253402257600, 'httpOnly': False, 'name': 'G_ENABLED_IDPS',
            'path': '/', 'secure': False, 'value': 'google'},
           {'domain': 'my.gumtree.com', 'httpOnly': True, 'name': 'GCLB', 'path': '/', 'secure': False,
            'value': 'CNOdlrH2lf3aPg'},
           {'domain': '.gumtree.com', 'expiry': 1644704903, 'httpOnly': True, 'name': 'gt_lcb', 'path': '/',
            'secure': True, 'value': ''},
           {'domain': 'my.gumtree.com', 'expiry': 1644707913, 'httpOnly': True, 'name': 'GTSELLERSESSIONID',
            'path': '/', 'secure': True, 'value': 'node014mphktboz3nxx45nosm13wd5346752.node0'},
           {'domain': '.gumtree.com', 'httpOnly': True, 'name': 'gt_tm', 'path': '/', 'secure': True,
            'value': '1f7ffa18-5592-4b86-bad7-ce9e28c2891d'},
           {'domain': '.my.gumtree.com', 'expiry': 1707776313, 'httpOnly': False, 'name': '_ga', 'path': '/',
            'secure': False, 'value': 'GA1.3.1604050776.1644704306'},
           {'domain': '.gumtree.com', 'expiry': 1802384313, 'httpOnly': True, 'name': 'gt_ab', 'path': '/',
            'secure': True, 'value': 'ln:ODVhd2M='},
           {'domain': '.gumtree.com', 'expiry': 1646000313, 'httpOnly': False, 'name': 'gt_appBanner', 'path': '/',
            'secure': True, 'value': ''},
           {'domain': '.gumtree.com', 'httpOnly': True, 'name': 'gt_s', 'path': '/', 'secure': True,
            'value': 'id:bm9kZTAxZHpjc3gzcWpkZGgycWdwcGF6ZzV4ejRiNzE5NTYyMg=='},
           {'domain': '.gumtree.com', 'expiry': 1802384313, 'httpOnly': True, 'name': 'gt_p', 'path': '/',
            'secure': True, 'value': 'id:Y2Y3MjQxNDgtMjRkOS00NjE4LTg1YWUtYWE1YmE4OTQzMWQx'},
           {'domain': '.my.gumtree.com', 'httpOnly': True, 'name': 'rbzsessionid', 'path': '/', 'secure': False,
            'value': 'be2f4d45e2d48d5d46b1fa5a53c1233c'},
           {'domain': '.my.gumtree.com', 'expiry': 1644790713, 'httpOnly': False, 'name': '_gid', 'path': '/',
            'secure': False, 'value': 'GA1.3.561542053.1644704306'}]
s = requests.Session()
for cookie in cookies:
    s.cookies.set(cookie['name'], cookie['value'])


async def start_parsing(message, conn):
    cur = conn.cursor()
    ID = message.from_user.id
    cur.execute('UPDATE stoped SET stop = %s WHERE id = %s', ('no', ID))
    conn.commit()

    cur.execute('SELECT data FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    data = cur.fetchone()[0]
    if data != 'No':
        year = re.findall('[0-9]+.[0-9]+.([0-9]+)', data)
        mesyac = re.findall('[0-9]+.([0-9]+).[0-9]+', data)
        day = re.findall('([0-9]+).[0-9]+.[0-9]+', data)
        data_re = year[0] + '-' + mesyac[0] + '-' + day[0]
    else:
        data_re = 'No'
    print(data_re)
    cur.execute('UPDATE bd_bot SET data = %s WHERE id = %s', (data_re, ID))

    now_all = datetime.now()

    cur.execute('SELECT url FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    URL_bd = cur.fetchone()[0]
    URL_bd = re.sub('\?.+', '', URL_bd)
    cur.execute('SELECT ot FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    PAGENATION_ot_bd = cur.fetchone()[0]
    cur.execute('SELECT pdo FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    PAGENATION_do_bd = cur.fetchone()[0]
    if PAGENATION_do_bd > 50:
        PAGENATION_do_bd = 50
    conn.commit()

    await bot.send_message(ID, 'Процесс парсинга запущен. Это может занять некоторое время', reply_markup=keyboard.stop())

    for page in range(PAGENATION_ot_bd, PAGENATION_do_bd):
        ID = message.from_user.id
        cur.execute('SELECT stop FROM stoped WHERE id = %s LIMIT 1', (ID,))
        stop_bd = cur.fetchone()[0]
        if stop_bd == 'yes':
            pages_print = 'Парсинг остановлен на странице ' + str(page)
            await bot.send_message(message.from_user.id, pages_print, reply_markup=keyboard.menu())
            break
        url = URL_bd + '/uk/page' + str(page)
        # pages_print='парсим страницу: '+str(page)
        # await bot.send_message(message.from_user.id, pages_print)
        # print(page)
        await first_step(url, str(message.from_user.id), conn, cur)
    await end(message, conn, cur, ID)


async def first_step(url, ID, conn, cur):
    cur.execute('SELECT data FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    DATA_bd = cur.fetchone()[0]
    now = datetime.now()
    cur.execute('SELECT proxy_list FROM proxy WHERE id = %s', ('123123',))
    proxy_list = cur.fetchall()[0][0]
    proxy_list = ast.literal_eval(proxy_list)
    proxy = random.choice(proxy_list)
    connector = ProxyConnector.from_url(proxy)
    HEADERS = {
        'authority': 'www.gumtree.com',
        'method': 'GET',
        'path': '/video-games-consoles?utm_source=featured_categories&utm_medium=All&utm_campaign=video-games-consoles',
        'scheme': 'https',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'max-age=0',
        'cookie': 'gt_ab=ln:ODJpcWc=; gt_p=id:OGQ2NmJhZmUtMThmMC00YWI1LTgyOTktMDY2MWVmOGU1ZWQx; GCLB=CPbzouTc2pTNEg; _ga=GA1.2.329248121.1638766962; _pubcid=ad57dd42-264b-4064-a58c-bc4752a6a7fb; OptanonAlertBoxClosed=2021-12-06T05:02:48.340Z; eupubconsent-v2=CPQx_6EPQx_6EAcABBENB4CsAP_AAAAAAAYgIGNf_X__b3_j-_59f_t0eY1P9_7_v-0zjhfdt-8N2f_X_L8X_2M7vF36pq4KuR4Eu3LBIQdlHOHcTUmw6okVrTPsbk2Mr7NKJ7PEmnMbO2dYGH9_n93T-ZKY7__9___7_v-_______f__-_____5_X---_f_V_99zLv9__3__9wAAAPAAAAkEgpgAIAAXABQAFQAMgAcAA8ACAAGEANAA1AB5AEMARQAmABPACqAFgAN4AcwA9ACEAENAIgAiYBLAEuAJoAUoAtwBhgDIAGqANkAd4A9gB8QD7AP0AgEBFwEYAI0ARwAlIBQQClgFPAKuAXMAxQBrADaAG4AOIAegBDYCHQEiAJiATKAmwBOwChwFIgLFAWwAuQBd4C8wGDAMJAYaAw8BkQDJAGTgMuAZyAz4BpADToGsAayA3WByIHKgOXAdGA6wB44D5QgA4AcwBhAFPgMmAdIA7AB2YDugHgAPKAe0A90B8gD7A0B0ALgAhgBkADZAH4AQAAjABSwCngFXgLQAtIBrAEOgJEATYAnYBSIC5AGEgMPAYwAycBnIDPAGfAOSAcoA6wB-AYAGAOYB2YD3RAAQAGoA5gHZgPdEQGQBDADIAGyAPwAgABGAClgFPAKuAawBDoCRAE2AJ2AUiAuQBhIDDwGTgM5AZ8A5IBygDrAH4CoDIAFAAhgBMAC4APwAjABHAClgFXgLQAtICQQExAJsAU2AtgBcgC8wGHgMiAZyAzwBnwDkgHKAPwFAAwBzADwAfYMgLgAUACGAEyAfYB-AEYAI4AUsAq4BWwExAJsAWiAtgBeYDDwGRAM5AZ4Az4ByQDlAHxAPwGABAAagDmAHgA-wdBsAAXABQAFQAMgAcABAAC6AGAAYwA0ADUAHgAPoAhgCKAEwAJ4AVQAsABcAC-AGIAMwAbwA5gB6AENAIgAiYBLAEwAJoAUYApQBYgC3gGEAYYAyABlADRAGyAN8Ad4A9oB9gH6AP-AiwCMAEcgJSAlQBQQCngFXALFAWgBaQC5gF5AMUAbQA3ABxADpgHoAQ2Ah0BEQCLwEggJEASoAmwBOwChwFNAKsAWKAtgBcAC5AF2gLvAXmAwYBhIDDQGHgMSAYwAx4BkgDJwGVAMsAZcAzkBnwDRIGkAaSA0sBpwDVQGsANjAbqA4uByQHKgOXAdGA6wB44D0gHqgPlAfWA_AcAOAHMAYQBkwDbAHIAOkAdgA7MB4ADygHtAPdAfEA-whA9AAWABQADIALgAYgBDACYAFUALgAXwAxABmADeAHoAWIAwgBvgDvgH2AfgA_wCMAEcAJSAUEAoYBTwCrwFoAWkAuYBigDaAHoASCAkQBKgCbAFNALFAWiAtgBbQC4AFyALtAYeAxIBkQDJwGcgM8AZ8A0QBpIDSwGqgOAAckA6MB1gDxwH4EABQA5gB4AGEAbYA7AB5QD0QHugPiAfYSgZgAIAAWABQADIAHIAYABiADwAIgATAAqgBcAC-AGIAMwAhoBEAESAKMAUoAtwBhADVAGyAO8AfgBGACOAFPAKvAWgBaQDFAG4AQ6Ai8BIgCbAFigLYAXaAvMBh4DIgGTgMsAZyAzwBnwDSAGsAOAAdYA_AkAGAHMA6QB2ADygHtAPsKQTAAFwAUABUADIAHAAQQAwADGAGgAagA8gCGAIoATAAngBSACqAFgAL4AYgAzABzAENAIgAiQBRgClAFiALcAYQAygBogDVAGyAO-AfYB-gEWAIwARwAlIBQQChgFXAK2AXMAvIBtADcAHoAQ6Ai8BIgCbAE7AKHAWKAtgBcAC5AF2gLzAYaAw8BjADIgGSAMnAZcAzkBngDPoGkAaTA1gDWQGxgN1gcmBygDlwHWAPHAfKA_AoAMAHMAPAAwgCnwGTAOwAdmA8oB7QD3QHxAPsAAA.f_gAAAAAAAAA; __gads=ID=10610f4754838c01:T=1638766968:S=ALNI_MbD0kO6mcdNebzvX023N8umE_SCpA; _fbp=fb.1.1638766984282.830993948; _lr_env_src_ats=false; AMCVS_9CD675E95BFD3B230A495C73%40AdobeOrg=1; gt_tm=27e73611-a8d6-44d7-a7e1-735d1d572fd3; gt_adconsent=state:Mw==; searchCount=2; AMCV_9CD675E95BFD3B230A495C73%40AdobeOrg=-1303530583%7CMCIDTS%7C18974%7CMCMID%7C63895136244360792111688784234399930453%7CMCAAMLH-1639912701%7C6%7CMCAAMB-1639912701%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1639315101s%7CNONE%7CvVersion%7C3.3.0%7CMCCIDH%7C634625902; ki_r=; gt_appBanner=; __gsas=ID=f93a9b45225a46fc:T=1644349349:S=ALNI_MY6vpuDACKzAPY1nzvz5Uk_9d2ZwA; gt_userPref=lfsk:Y3JpdGljYWwuY3NzLm1hcCxpcGhvbmUsc3ZhLHNh|isSearchOpen:ZmFsc2U=|recentAdsOne:Y2Fycy12YW5zLW1vdG9yYmlrZXM=|cookiePolicy:dHJ1ZQ==|recentAdsTwo:Zm9yLXNhbGU=|location:dWs=; eCG_eh=ec=ResultsBrowse:ea=NavL1CategoryHoover:el=; _gcl_au=1.1.1283021334.1646665315; gt_rememberMe=kMAIZF28X0IAqDaoeHuoFXdB93k6J64sb/YtKJg56vX98kX0yy0GgIVFmplMoygIXv3GaIxMfPkvag7T0qroct7h9L1OY8ynNL4YQQqEHK8jReD673Y/qEGZqfzQiWGF8MExUvOrHMPCEP8sULIzzkjx+mqfsh1nYMgBI/3Iwvg=; gt_mc=rcd:MA==|nuc:MA==; lux_uid=164703840698331187; _gid=GA1.2.1772725439.1647038409; _gat=1; _pbjs_userid_consent_data=4113121600677992; rbzid=pnYh2Ca2gvlJgRoizb/QZ5cOlvztEVI/RlWbWYy4GWMIxV+toluDJXfKPFlit3eJF3iilBwDlIdF2p5mUSz725A1MHnK+1Y/rKvZU6MNzCN+fZAJUzWNSpeCjLzHZ/e8AjfDQyTOtk4TjimR01yLJ7SLMpqfqi45McZIqx7hPOqv+Y2dl/klAYKUY/koo3AFiDw9d1UMhEfgj6w4iWgFKylsAdk1xNyCZzu1LYyOiNISNkZrcuTq8KoOMdWePr3kP9huGUBnFuDUcjfTIZQnDx3HZMaQkvlNn7nPeLwWjsk=; rbzsessionid=06ad918b04a19161ba06a251fd14b32a; _lr_retry_request=true; gt_s=sc:MjQ4OA==|ar:aHR0cDovL3d3dy5ndW10cmVlLmNvbS92aWRlby1nYW1lcy1jb25zb2xlcz91dG1fc291cmNlPWZlYXR1cmVkX2NhdGVnb3JpZXMmdXRtX21lZGl1bT1BbGwmdXRtX2NhbXBhaWduPXZpZGVvLWdhbWVzLWNvbnNvbGVz|st:MTY0NzAzODQyNzMzMQ==|clicksource_featured:MTQxODY1MTk2NywxNDI2Nzg3MjUwLDE0Mjc1NTcyNjIsMTQyNjIyMDU4MCwxNDI3NjU4NzUy|sk:|bci:MkM4NTg3RTY3RDM1Njg0NzNBMjM5NkU4N0M2MjY5RjQ=|clicksource_nearby:|id:bm9kZTAxdGl1dHRwbndlb3QxcTU2djU4Z3Jwb25lMjI2MzI=|clicksource_natural:MTQyNzcxODQxNiwxNDI3NzE4NDAwLDE0Mjc3MTgzOTksMTQyNzcxODM1MiwxNDI3NzE4MzM4LDE0Mjc3MTgyNzcsMTQyNzcxODI2NCwxNDI3NzE4MTU0LDE0Mjc3MTgwNTUsMTQyNTM4NDM0OSwxNDI3NzE4MDI3LDE0Mjc3MTgwMTUsMTQyNzcxNzk0OCwxNDI3NzE3ODk0LDE0MjMwNDkzNDMsMTQyNzcxNzg2MiwxNDE3MzA3OTgwLDE0Mjc3MTc2NzksMTQwNjUzNDUxOSwxNDI3NzE3NTMzLDE0Mjc3MTc0ODEsMTQyMzE4OTc2MSwxNDE3Njg3NTMwLDE0MTc2ODcyMDksMTQxNzY4NzY4NA==; gt_userIntr=cnt:Mg==; OptanonConsent=geolocation=RU%3BKDA&datestamp=Sat+Mar+12+2022+01%3A40%3A27+GMT%2B0300+(%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0%2C+%D1%81%D1%82%D0%B0%D0%BD%D0%B4%D0%B0%D1%80%D1%82%D0%BD%D0%BE%D0%B5+%D0%B2%D1%80%D0%B5%D0%BC%D1%8F)&version=6.10.0&isIABGlobal=false&hosts=&groups=STACK42%3A1%2CFACEB%3A0%2CLIVER%3A0%2CC0023%3A0%2CC0026%3A0%2CC0028%3A0%2CC0029%3A0%2CGAPTS%3A0&consentId=c70050ae-887b-478c-8728-0f46e7c9fc01&interactionCount=1&landingPath=NotLandingPage&AwaitingReconsent=false; cto_bundle=4iri4F9MeFRIQnpSb2FmZHlWVGlIbWlidGpKdmpxa055ZlN3Qm5zcVBLY29MOFNYZDJRNWFNQmN4MG9Lb0VXSmMlMkJxSEpjZkpBbUVPcndnTGtOVDdTT1U0UlJxbGxRcnJLZUptR0VBWnI4VFFHSllZTmd4Y2pSYUN3YXRTRFlzS1h2Y0xSekNZTTdtamZXNXU3OUdCQkY2MTR4dyUzRCUzRA',
        'referer': 'https://www.gumtree.com/',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'
    }
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(url, headers=HEADERS) as resp:
            html = await resp.text()
    items_list = []
    items_dict = {}
    soup = BeautifulSoup(html, 'lxml')
    f = open('soup_222.txt', 'w+', encoding='utf-8')
    f.write(url)
    f.write(str(soup))
    f.close()
    items = soup.find_all('article', class_="listing-maxi")
    count = 0
    for item in items:
        cur.execute('SELECT all_count FROM num WHERE id = %s LIMIT 1', (ID,))
        all_count_ = int(cur.fetchone()[0]) + 1
        cur.execute('UPDATE num SET all_count = %s WHERE id = %s', (all_count_, ID))
        if count == 0:
            count += 1
            continue
        if DATA_bd == 'No':
            data_re = 'No'
        else:
            try:
                data = item.find('span', class_="truncate-line txt-tertiary").get_text()
            except:
                print('data_error')
                continue
            if data.find('days') != -1:
                days_ = re.findall('([0-9]+) .+', data)
                delta = timedelta(days=int(days_[0]))
                data_all = now - delta
                data_re = re.findall('(\S+) .+', str(data_all))
            else:
                data_re = re.findall('(\S+) .+', str(now))
            data_re = data_re[0]
        if DATA_bd == data_re:
            links = HOST + item.find('a', class_="listing-link").get('href')
            product_img = soup.find('div', class_="listing-thumbnail").find('img').get('src')
            title = item.find('h2', class_="listing-title").get_text()
            price = item.find('strong', class_="h3-responsive").get_text(strip=True)
            id_links = re.findall('.+/([0-9]+)', links)
            item_dict = {links: {
                'link': links,
                'id_links': id_links[0],
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
    print(len(items_list))
    print(f'first_step compleat at {ID}')
    await second_step(items_list, items_dict, ID, session, conn, cur)


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


async def fetch_account(session, account_id):
    url = f'https://www.gumtree.com/profile/accounts/{account_id}'
    headers = {
        'authority': 'www.gumtree.com',
        'method': 'GET',
        'path': '/profile/accounts/70dfd52ef9bba6f2dcb7a3040012ef29',
        'scheme': 'https',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'cookie': 'eCG_eh=ec=UserStatic:ea=logoutDropdown:el=; gt_ab=ln:ODJpcWc=; gt_p=id:OGQ2NmJhZmUtMThmMC00YWI1LTgyOTktMDY2MWVmOGU1ZWQx; GCLB=CPbzouTc2pTNEg; _ga=GA1.2.329248121.1638766962; _pubcid=ad57dd42-264b-4064-a58c-bc4752a6a7fb; OptanonAlertBoxClosed=2021-12-06T05:02:48.340Z; eupubconsent-v2=CPQx_6EPQx_6EAcABBENB4CsAP_AAAAAAAYgIGNf_X__b3_j-_59f_t0eY1P9_7_v-0zjhfdt-8N2f_X_L8X_2M7vF36pq4KuR4Eu3LBIQdlHOHcTUmw6okVrTPsbk2Mr7NKJ7PEmnMbO2dYGH9_n93T-ZKY7__9___7_v-_______f__-_____5_X---_f_V_99zLv9__3__9wAAAPAAAAkEgpgAIAAXABQAFQAMgAcAA8ACAAGEANAA1AB5AEMARQAmABPACqAFgAN4AcwA9ACEAENAIgAiYBLAEuAJoAUoAtwBhgDIAGqANkAd4A9gB8QD7AP0AgEBFwEYAI0ARwAlIBQQClgFPAKuAXMAxQBrADaAG4AOIAegBDYCHQEiAJiATKAmwBOwChwFIgLFAWwAuQBd4C8wGDAMJAYaAw8BkQDJAGTgMuAZyAz4BpADToGsAayA3WByIHKgOXAdGA6wB44D5QgA4AcwBhAFPgMmAdIA7AB2YDugHgAPKAe0A90B8gD7A0B0ALgAhgBkADZAH4AQAAjABSwCngFXgLQAtIBrAEOgJEATYAnYBSIC5AGEgMPAYwAycBnIDPAGfAOSAcoA6wB-AYAGAOYB2YD3RAAQAGoA5gHZgPdEQGQBDADIAGyAPwAgABGAClgFPAKuAawBDoCRAE2AJ2AUiAuQBhIDDwGTgM5AZ8A5IBygDrAH4CoDIAFAAhgBMAC4APwAjABHAClgFXgLQAtICQQExAJsAU2AtgBcgC8wGHgMiAZyAzwBnwDkgHKAPwFAAwBzADwAfYMgLgAUACGAEyAfYB-AEYAI4AUsAq4BWwExAJsAWiAtgBeYDDwGRAM5AZ4Az4ByQDlAHxAPwGABAAagDmAHgA-wdBsAAXABQAFQAMgAcABAAC6AGAAYwA0ADUAHgAPoAhgCKAEwAJ4AVQAsABcAC-AGIAMwAbwA5gB6AENAIgAiYBLAEwAJoAUYApQBYgC3gGEAYYAyABlADRAGyAN8Ad4A9oB9gH6AP-AiwCMAEcgJSAlQBQQCngFXALFAWgBaQC5gF5AMUAbQA3ABxADpgHoAQ2Ah0BEQCLwEggJEASoAmwBOwChwFNAKsAWKAtgBcAC5AF2gLvAXmAwYBhIDDQGHgMSAYwAx4BkgDJwGVAMsAZcAzkBnwDRIGkAaSA0sBpwDVQGsANjAbqA4uByQHKgOXAdGA6wB44D0gHqgPlAfWA_AcAOAHMAYQBkwDbAHIAOkAdgA7MB4ADygHtAPdAfEA-whA9AAWABQADIALgAYgBDACYAFUALgAXwAxABmADeAHoAWIAwgBvgDvgH2AfgA_wCMAEcAJSAUEAoYBTwCrwFoAWkAuYBigDaAHoASCAkQBKgCbAFNALFAWiAtgBbQC4AFyALtAYeAxIBkQDJwGcgM8AZ8A0QBpIDSwGqgOAAckA6MB1gDxwH4EABQA5gB4AGEAbYA7AB5QD0QHugPiAfYSgZgAIAAWABQADIAHIAYABiADwAIgATAAqgBcAC-AGIAMwAhoBEAESAKMAUoAtwBhADVAGyAO8AfgBGACOAFPAKvAWgBaQDFAG4AQ6Ai8BIgCbAFigLYAXaAvMBh4DIgGTgMsAZyAzwBnwDSAGsAOAAdYA_AkAGAHMA6QB2ADygHtAPsKQTAAFwAUABUADIAHAAQQAwADGAGgAagA8gCGAIoATAAngBSACqAFgAL4AYgAzABzAENAIgAiQBRgClAFiALcAYQAygBogDVAGyAO-AfYB-gEWAIwARwAlIBQQChgFXAK2AXMAvIBtADcAHoAQ6Ai8BIgCbAE7AKHAWKAtgBcAC5AF2gLzAYaAw8BjADIgGSAMnAZcAzkBngDPoGkAaTA1gDWQGxgN1gcmBygDlwHWAPHAfKA_AoAMAHMAPAAwgCnwGTAOwAdmA8oB7QD3QHxAPsAAA.f_gAAAAAAAAA; __gads=ID=10610f4754838c01:T=1638766968:S=ALNI_MbD0kO6mcdNebzvX023N8umE_SCpA; _fbp=fb.1.1638766984282.830993948; _lr_env_src_ats=false; AMCVS_9CD675E95BFD3B230A495C73%40AdobeOrg=1; gt_tm=27e73611-a8d6-44d7-a7e1-735d1d572fd3; gt_adconsent=state:Mw==; searchCount=2; AMCV_9CD675E95BFD3B230A495C73%40AdobeOrg=-1303530583%7CMCIDTS%7C18974%7CMCMID%7C63895136244360792111688784234399930453%7CMCAAMLH-1639912701%7C6%7CMCAAMB-1639912701%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1639315101s%7CNONE%7CvVersion%7C3.3.0%7CMCCIDH%7C634625902; ki_r=; gt_appBanner=; __gsas=ID=f93a9b45225a46fc:T=1644349349:S=ALNI_MY6vpuDACKzAPY1nzvz5Uk_9d2ZwA; gt_userPref=lfsk:Y3JpdGljYWwuY3NzLm1hcCxpcGhvbmUsc3ZhLHNh|isSearchOpen:ZmFsc2U=|recentAdsOne:Y2Fycy12YW5zLW1vdG9yYmlrZXM=|cookiePolicy:dHJ1ZQ==|recentAdsTwo:Zm9yLXNhbGU=|location:dWs=; eCG_eh=ec=ResultsBrowse:ea=NavL1CategoryHoover:el=; _gid=GA1.2.1815664873.1646402962; gt_rememberMe=4wFNrbBAWL3isY8UPBkgEvovGYdpljynI04x6EDv31emrfFR92vWu9JXAnP/mYYIM8xQuSvyNrhGzv3Bwfo671+9+RJ4c75e96v3f/9TEvub8OR5b7zmmXD0Hb7nXJlh8nGeig5fh5J3zkDhp16BiXgpgOvJlVhUR++te1lunYCT/DxZLo3wmr/b9MGUad2g; gt_mc=rcd:MA==|nuc:MA==; rbzsessionid=f1da1f610c98a5c155848cdf6394a719; rbzid=zmemFtf2VYnVjN0wpesK9G/YyAEQoe+1EHCc1YXx/rhUirDAXKJ1NTy+mjkDToJ35anlou0TEPABydgixftG5dvQa1N2HYwTPxhWtVyTqHqooueCForWjfDzFx/35KRck6alNMg8yRQsqWlfcM46HfdrXU35pz5BY2iMjNJtdJZLNMmrr2TEGqUUJGLV5suW0beqYhV8EjLrGn7fGDw9J+waY0zcziKDuWwAVbau10JmCPYrMdaQ4a9Q6+gX/vD5LlLl/ZzuZb5BwpplUQpeQ1tuyEcerFCCbxENpE1JeSR9evbb/RZ9+it4sKO5RUkC; lux_uid=164659927947905286; _lr_retry_request=true; gt_s=sc:MjUxNQ==|ar:aHR0cDovL3d3dy5ndW10cmVlLmNvbS9jb21wdXRlcnMtc29mdHdhcmU=|st:MTY0NjU5OTQxODY2MQ==|clicksource_featured:MTQyNzE4NTk0MCwxNDI2NjU3Nzc4LDE0MjcxNjMyOTUsMTQyNzI5MDEyNiwxNDI3MDQxNTY3|bci:MkM4NTg3RTY3RDM1Njg0NzNBMjM5NkU4N0M2MjY5RjQ=|id:bm9kZTAxdGl1dHRwbndlb3QxcTU2djU4Z3Jwb25lMjI2MzI=|clicksource_natural:MTQyNzM1NjE4NywxNDI3MzU2MTAxLDE0MjczNTYwOTYsMTQyNTE2NTAwNywxNDI3MzU2MDE5LDE0MjczNTU4NzksMTQyNzM1NTg1NywxNDIyMzY2NTU5LDE0MjczNTU4MjYsMTQxNTI5MTc3MCwxNDI3MzU1ODExLDE0MjczNTU4MDAsMTQyNzM1NTc3NywxNDI3MzU1Nzc0LDE0MjczNTU3MzYsMTQyNzM1NTcyMiwxNDI3MzU1NzA1LDE0MjczNTU2OTAsMTQyMTc5NDI0MiwxNDI3MzU1NDkwLDE0MjczNTU0MTQsMTM3MzUwNDEyMywxNDI3MzU1MjA2LDE0MjczNTUxOTcsMTQyNzM1NTEzNQ==; gt_mc="rcd:MTY0NjU5OTg2MDY2Nw==|nuc:MA=="; gt_userIntr=cnt:MTk=; _gat=1; OptanonConsent=geolocation=RU%3BKDA&datestamp=Sun+Mar+06+2022+23%3A48%3A16+GMT%2B0300+(%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0%2C+%D1%81%D1%82%D0%B0%D0%BD%D0%B4%D0%B0%D1%80%D1%82%D0%BD%D0%BE%D0%B5+%D0%B2%D1%80%D0%B5%D0%BC%D1%8F)&version=6.10.0&isIABGlobal=false&hosts=&groups=STACK42%3A1%2CFACEB%3A0%2CLIVER%3A0%2CC0023%3A0%2CC0026%3A0%2CC0028%3A0%2CC0029%3A0%2CGAPTS%3A0&consentId=c70050ae-887b-478c-8728-0f46e7c9fc01&interactionCount=1&landingPath=NotLandingPage&AwaitingReconsent=false; _pbjs_userid_consent_data=6715805462517114; cto_bundle=v5x_jV9MeFRIQnpSb2FmZHlWVGlIbWlidGpJZlFzbEUwNVRYN0VldldvYUVGWEo2bkM5Um53UkdMWHJ5NTRoWDllcEczZXpCOVp6NzhwNSUyQjVtY0tRcjZGYWFGQUM1VWdUZGJuSzlvdFdWZmZDM2pONCUyQnAxbSUyQkdON295c1ByVGpEY0R2dG9FbE5jS2tES1E0T2N4VmtadnUwNHclM0QlM0Q',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'
    }
    async with session.get(url, ssl=ssl.SSLContext(), headers=headers) as resp:
        try:
            return await resp.text()
        except:
            print('utf-8 encoding_error')
            return 'none'


async def second_step(items_list, items_dict, ID, session, conn, cur):
    cookie = {
        'cookie': 'gt_ab=ln:ODJpcWc=; gt_p=id:OGQ2NmJhZmUtMThmMC00YWI1LTgyOTktMDY2MWVmOGU1ZWQx; GCLB=CPbzouTc2pTNEg; _ga=GA1.2.329248121.1638766962; _pubcid=ad57dd42-264b-4064-a58c-bc4752a6a7fb; OptanonAlertBoxClosed=2021-12-06T05:02:48.340Z; eupubconsent-v2=CPQx_6EPQx_6EAcABBENB4CsAP_AAAAAAAYgIGNf_X__b3_j-_59f_t0eY1P9_7_v-0zjhfdt-8N2f_X_L8X_2M7vF36pq4KuR4Eu3LBIQdlHOHcTUmw6okVrTPsbk2Mr7NKJ7PEmnMbO2dYGH9_n93T-ZKY7__9___7_v-_______f__-_____5_X---_f_V_99zLv9__3__9wAAAPAAAAkEgpgAIAAXABQAFQAMgAcAA8ACAAGEANAA1AB5AEMARQAmABPACqAFgAN4AcwA9ACEAENAIgAiYBLAEuAJoAUoAtwBhgDIAGqANkAd4A9gB8QD7AP0AgEBFwEYAI0ARwAlIBQQClgFPAKuAXMAxQBrADaAG4AOIAegBDYCHQEiAJiATKAmwBOwChwFIgLFAWwAuQBd4C8wGDAMJAYaAw8BkQDJAGTgMuAZyAz4BpADToGsAayA3WByIHKgOXAdGA6wB44D5QgA4AcwBhAFPgMmAdIA7AB2YDugHgAPKAe0A90B8gD7A0B0ALgAhgBkADZAH4AQAAjABSwCngFXgLQAtIBrAEOgJEATYAnYBSIC5AGEgMPAYwAycBnIDPAGfAOSAcoA6wB-AYAGAOYB2YD3RAAQAGoA5gHZgPdEQGQBDADIAGyAPwAgABGAClgFPAKuAawBDoCRAE2AJ2AUiAuQBhIDDwGTgM5AZ8A5IBygDrAH4CoDIAFAAhgBMAC4APwAjABHAClgFXgLQAtICQQExAJsAU2AtgBcgC8wGHgMiAZyAzwBnwDkgHKAPwFAAwBzADwAfYMgLgAUACGAEyAfYB-AEYAI4AUsAq4BWwExAJsAWiAtgBeYDDwGRAM5AZ4Az4ByQDlAHxAPwGABAAagDmAHgA-wdBsAAXABQAFQAMgAcABAAC6AGAAYwA0ADUAHgAPoAhgCKAEwAJ4AVQAsABcAC-AGIAMwAbwA5gB6AENAIgAiYBLAEwAJoAUYApQBYgC3gGEAYYAyABlADRAGyAN8Ad4A9oB9gH6AP-AiwCMAEcgJSAlQBQQCngFXALFAWgBaQC5gF5AMUAbQA3ABxADpgHoAQ2Ah0BEQCLwEggJEASoAmwBOwChwFNAKsAWKAtgBcAC5AF2gLvAXmAwYBhIDDQGHgMSAYwAx4BkgDJwGVAMsAZcAzkBnwDRIGkAaSA0sBpwDVQGsANjAbqA4uByQHKgOXAdGA6wB44D0gHqgPlAfWA_AcAOAHMAYQBkwDbAHIAOkAdgA7MB4ADygHtAPdAfEA-whA9AAWABQADIALgAYgBDACYAFUALgAXwAxABmADeAHoAWIAwgBvgDvgH2AfgA_wCMAEcAJSAUEAoYBTwCrwFoAWkAuYBigDaAHoASCAkQBKgCbAFNALFAWiAtgBbQC4AFyALtAYeAxIBkQDJwGcgM8AZ8A0QBpIDSwGqgOAAckA6MB1gDxwH4EABQA5gB4AGEAbYA7AB5QD0QHugPiAfYSgZgAIAAWABQADIAHIAYABiADwAIgATAAqgBcAC-AGIAMwAhoBEAESAKMAUoAtwBhADVAGyAO8AfgBGACOAFPAKvAWgBaQDFAG4AQ6Ai8BIgCbAFigLYAXaAvMBh4DIgGTgMsAZyAzwBnwDSAGsAOAAdYA_AkAGAHMA6QB2ADygHtAPsKQTAAFwAUABUADIAHAAQQAwADGAGgAagA8gCGAIoATAAngBSACqAFgAL4AYgAzABzAENAIgAiQBRgClAFiALcAYQAygBogDVAGyAO-AfYB-gEWAIwARwAlIBQQChgFXAK2AXMAvIBtADcAHoAQ6Ai8BIgCbAE7AKHAWKAtgBcAC5AF2gLzAYaAw8BjADIgGSAMnAZcAzkBngDPoGkAaTA1gDWQGxgN1gcmBygDlwHWAPHAfKA_AoAMAHMAPAAwgCnwGTAOwAdmA8oB7QD3QHxAPsAAA.f_gAAAAAAAAA; __gads=ID=10610f4754838c01:T=1638766968:S=ALNI_MbD0kO6mcdNebzvX023N8umE_SCpA; _fbp=fb.1.1638766984282.830993948; _lr_env_src_ats=false; AMCVS_9CD675E95BFD3B230A495C73%40AdobeOrg=1; gt_tm=27e73611-a8d6-44d7-a7e1-735d1d572fd3; gt_adconsent=state:Mw==; searchCount=2; AMCV_9CD675E95BFD3B230A495C73%40AdobeOrg=-1303530583%7CMCIDTS%7C18974%7CMCMID%7C63895136244360792111688784234399930453%7CMCAAMLH-1639912701%7C6%7CMCAAMB-1639912701%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1639315101s%7CNONE%7CvVersion%7C3.3.0%7CMCCIDH%7C634625902; ki_r=; gt_appBanner=; __gsas=ID=f93a9b45225a46fc:T=1644349349:S=ALNI_MY6vpuDACKzAPY1nzvz5Uk_9d2ZwA; gt_userPref=lfsk:Y3JpdGljYWwuY3NzLm1hcCxpcGhvbmUsc3ZhLHNh|isSearchOpen:ZmFsc2U=|recentAdsOne:Y2Fycy12YW5zLW1vdG9yYmlrZXM=|cookiePolicy:dHJ1ZQ==|recentAdsTwo:Zm9yLXNhbGU=|location:dWs=; eCG_eh=ec=ResultsBrowse:ea=NavL1CategoryHoover:el=; _gid=GA1.2.1815664873.1646402962; gt_s=sc:MTExMTQ=|ar:aHR0cDovL3d3dy5ndW10cmVlLmNvbS9raXRjaGVud2FyZS1hY2Nlc3Nvcmllcw==|st:MTY0NjQ3ODg1NDgxOQ==|clicksource_featured:MTQyNjcwNTE5MywxNDI3MTA2MjI3LDE0MjY3ODYzMDUsMTQyNzE5Mjk4NSwxNDIzODM0NzM0|bci:MkM4NTg3RTY3RDM1Njg0NzNBMjM5NkU4N0M2MjY5RjQ=|id:bm9kZTAxdGl1dHRwbndlb3QxcTU2djU4Z3Jwb25lMjI2MzI=|clicksource_natural:MTQyMjg2Nzc4MCwxNDIzMzg0MTkxLDE0MjcyMzU0NjIsMTQyNzIzNTM5OCwxNDI0NDMzNTYyLDE0MjQ0MzMxNDAsMTQyNzIzNTI4NCwxNDI3MjM1MTI1LDE0MjcyMzQ5NjEsMTQxMjE2MDc1NiwxNDI3MjM0ODAxLDE0MjI4NjY0MDEsMTM2NTM2MzI4MCwxNDExMjQ3OTYyLDE0MTkyOTY2MzksMTM4MjY3MzU0NCwxNDI3MjM0MzcxLDE0MjI3NjcyMDcsMTQyMjc2Njk5MywxNDIyNzY2MTQ2LDE0MjI3NjU5NTQsMTQwNDU1NzM3NSwxNDIyNzc1Mjk3LDEzNzk3NjQwMTAsMTQyNzIzNDE0MA==; _pbjs_userid_consent_data=6715805462517114; rbzsessionid=385a8da95f9097b3a9fec4568a2b65f2; gt_rememberMe=4wFNrbBAWL3isY8UPBkgEvovGYdpljynI04x6EDv31emrfFR92vWu9JXAnP/mYYIM8xQuSvyNrhGzv3Bwfo671+9+RJ4c75e96v3f/9TEvub8OR5b7zmmXD0Hb7nXJlh8nGeig5fh5J3zkDhp16BiXgpgOvJlVhUR++te1lunYCT/DxZLo3wmr/b9MGUad2g; gt_mc=rcd:MA==|nuc:MA==; rbzid=jPnehhQE9gQGwT1SFse5iSvF0YWmWy4pA4AgNjPyJL98Ja2yQTZK/v2BVhzXcGdMVHJsyEQ/120jP4LP6qhM8+iKK1jHP3l1JjKDKCvJAvChRpIO2jv8PPQ56IYtyKNrn4D2N3pfOUi9gWhVpqYgBWdilkwjVykmYB2vUDzjl2RAxDpqnCl6YJ8+M0p4/9vXceTh0QAU/ucMFqvnB9DSW6Dbfje+VJvAeD1Fd64OiHnPiecT2qUph0ZTzp1UOxBrX8qfwGJpdHvfmoO+O6pop7r/gLeuPF15JgIwnFEaCeTjN7hT7N8wBK6+4Mmyx8SHdZx6JMFLutws7TB5/BZEOw==; lux_uid=164657440687953147; _gat=1; OptanonConsent=geolocation=RU%3BKDA&datestamp=Sun+Mar+06+2022+16%3A47%3A55+GMT%2B0300+(%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0%2C+%D1%81%D1%82%D0%B0%D0%BD%D0%B4%D0%B0%D1%80%D1%82%D0%BD%D0%BE%D0%B5+%D0%B2%D1%80%D0%B5%D0%BC%D1%8F)&version=6.10.0&isIABGlobal=false&hosts=&groups=STACK42%3A1%2CFACEB%3A0%2CLIVER%3A0%2CC0023%3A0%2CC0026%3A0%2CC0028%3A0%2CC0029%3A0%2CGAPTS%3A0&consentId=c70050ae-887b-478c-8728-0f46e7c9fc01&interactionCount=1&landingPath=NotLandingPage&AwaitingReconsent=false; cto_bundle=cPpkG19MeFRIQnpSb2FmZHlWVGlIbWlidGpNZ1lTeFFQY2J0Tm9tdVFldmpqTWtTS2t3Mzk5a3pmV1BRalo3bEJrYzNackJUMjh2R2JaMFYzQ0VNRUN3VVNOQUFmY1BxUFZTbDFjZWFJOWd6TUlJSUxqempCdXZZd1Vxbkg4QXhGc1FTcDhsRTl2TU56UnhXeUhabGVCYjRuZ0ElM0QlM0Q; gt_userIntr=cnt:Ng==; gt_mc="rcd:MTY0NjU3NDkwMzg1MA==|nuc:MA=='}
    loop = asyncio.get_event_loop()
    async with aiohttp.ClientSession(loop=loop, cookies=cookie) as session_loop:
        html_links = await asyncio.gather(*[fetch(session_loop, url) for url in items_list])
        items_dict1 = {}
        items_list1 = []
        for item in html_links:
            soup = BeautifulSoup(item, 'lxml')
            try:
                phone = soup.find('h2', class_="seller-phone-number-title").get_text()
            except:
                phone = 'None'
            if phone != 'None':
                link = HOST + soup.find('a', class_="link", string=re.compile('View Profile')).get('href')
                name = soup.find('span', itemprop="name").get_text(strip=True).lower()
                token = re.findall('rt=(.+?)"', str(item))
                key_for_list = soup.find('link', rel="canonical").get('href')
                key_for_list = re.sub('https://www.gumtree.com', 'http://gumtree.com', key_for_list)
                items_dict[key_for_list].update({
                    'token': token[0],
                })
                dict_ = {name: items_dict[key_for_list]}
                items_dict1.update(dict_)
                account_id = re.findall('http://gumtree.com/profile/accounts/(.+)', link)
                items_list1.append(account_id[0])
            else:
                cur.execute('SELECT error1 FROM num WHERE id = %s LIMIT 1', (ID,))
                error1_ = int(cur.fetchone()[0]) + 1
                cur.execute('UPDATE num SET error1 = %s WHERE id = %s', (error1_, ID))
        print(f'second_step compleat at {ID}')
        await third_step(items_list1, items_dict1, ID, session_loop, cur)


async def third_step(items_list1, items_dict1, ID, session_loop, cur):
    cur.execute('SELECT opit FROM bd_bot WHERE id = %s LIMIT 1', (ID,))
    opit_bd = cur.fetchone()[0]
    cur.execute('SELECT _text_ FROM wtext WHERE id = %s LIMIT 1', (ID,))
    url_text = cur.fetchone()[0]
    if url_text is None:
        url_text = 'Hallo,+ist+diese+Anzeige+noch+aktuell? '
    html_links = await asyncio.gather(*[fetch_account(session_loop, url) for url in items_list1])
    dabl_url = []
    for item in html_links:
        if item == 'none':
            continue
        soup = BeautifulSoup(item, 'lxml')
        how_many = soup.find('div', class_="live-ads-profile").find('h2').get_text(strip=True)
        how_many_tovar_re = re.findall('.+ ([0-9]+) .+', str(how_many))
        how_many_tovar = int(how_many_tovar_re[0])
        name = re.findall('(.+).s [0-9]', str(how_many))
        link = name[0].lower().strip()
        if opit_bd >= how_many_tovar:
            cur.execute('SELECT success FROM num WHERE id = %s LIMIT 1', (ID,))
            success_ = int(cur.fetchone()[0]) + 1
            cur.execute('UPDATE num SET success = %s WHERE id = %s', (success_, ID))
            try:
                phone = await get_phone(items_dict1[link]['link'])
                print(phone)
            except:
                continue
            if dabl_url.count(str(items_dict1[link]['link'])) > 0:
                continue
            else:
                dabl_url.append(str(items_dict1[link]['link']))
            message_text = ('<code>' + str(items_dict1[link]['title']) + '</code>' + '\nссылка: ' + str(
                items_dict1[link]['link']) + '\nцена: ' + '<code>' + str(
                items_dict1[link]['price']) + '</code>' + '\nкартинка: ' + str(
                items_dict1[link]['img']) + '\nколичество сделок у продавца: ' + str(
                how_many_tovar) + '\nдата размещения объявления: ' + str(
                items_dict1[link]['data']) + '\nтелефон: ' + '<code>44' + str(phone)) + '</code>'
            await bot.send_message(ID, message_text, parse_mode="HTML", reply_markup=keyboard.wats(phone, url_text, items_dict1[link]['link']))
        else:
            cur.execute('SELECT error3 FROM num WHERE id = %s LIMIT 1', (ID,))
            error3_ = int(cur.fetchone()[0]) + 1
            cur.execute('UPDATE num SET error3 = %s WHERE id = %s', (error3_, ID))
    print(f'third_step compleat at {ID}')


async def get_phone(url):
    HEADERS = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'
    }
    url = url + '?srn=true'
    rool = 'true'
    while rool == 'true':
        try:
            html = s.get(url, headers=HEADERS, timeout=5)
            soup = BeautifulSoup(html.text, 'lxml')
            phone = soup.find('h2', class_="seller-phone-number-title").get_text()
            return phone
        except:
            print('error_timeout')


async def end(message, cur, ID):
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
