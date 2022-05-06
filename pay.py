# import sqlite3
import psycopg2
from datetime import datetime


def user_exists(conn, cur, ID):
    try:
        cur.execute('SELECT * FROM users WHERE id = %s', (ID,))
        result = cur.fetchall()
        return bool(len(result))
    except:
        conn.rollback()
        return False


def add_user(conn, cur, ID):
    now = datetime.now()
    cur.execute('INSERT INTO users (id, sail, money, time_sub) VALUES (%s, %s, %s, %s)', (ID, 0, 0, now))
    cur.execute('INSERT INTO stoped (ID, stop) VALUES (%s, %s)', (ID, 'no'))
    cur.execute('INSERT INTO wtext (ID) VALUES (%s)', (ID,))
    conn.commit()


def add_check(conn, cur, ID, money, bill_id):
    cur.execute('INSERT INTO chek (id, money, bill_id) VALUES (%s, %s, %s)', (ID, money, bill_id))
    conn.commit()


def get_check(cur, conn, bill_id):
    try:
        cur.execute('SELECT * FROM chek WHERE bill_id = %s', (bill_id,))
        result = cur.fetchall()
    except:
        conn.rollback()
        result = ''
    if not bool(len(result)):
        return False
    return result


def delete_check(conn, cur, bill_id):
    cur.execute('DELETE FROM chek WHERE bill_id = %s', (bill_id,))
    conn.commit()


def get_bill(cur, conn, ID):
    try:
        cur.execute('SELECT bill_id FROM chek WHERE id = %s', (ID,))
        result = cur.fetchall()
    except:
        conn.rollback()
        result = ''
    if not bool(len(result)):
        return False
    return result[0][0]


def user_money(cur, conn, ID):
    try:
        cur.execute('SELECT money FROM users WHERE id = %s', (ID,))
        result = cur.fetchall()
    except:
        conn.rollback()
        result = []
    if result == []:
        return 0
    if result[0][0] is None:
        return 0
    else:
        return result[0][0]


def set_money(conn, cur, ID, money):
    cur.execute('UPDATE users SET money = %s WHERE id = %s', (money, ID))
    conn.commit()


def check_true_promo(cur, conn, promo):
    try:
        cur.execute('SELECT promokods FROM promo')
        promokods = eval(cur.fetchall()[0][0])
    except:
        conn.rollback()
        promokods = 'no'
    if promo in promokods:
        return int(promokods[promo])
    else:
        return False


def users_sail_update(conn, cur, sail, ID):
    cur.execute('UPDATE users SET sail = %s WHERE id = %s', (sail, ID))
    conn.commit()


def update_promokods(conn, cur, promokods, ID):
    print(promokods)
    cur.execute('UPDATE promo SET promokods = %s WHERE id = %s', (promokods, ID))
    conn.commit()


def sail(cur, ID, money):
    cur.execute('SELECT sail FROM users WHERE id = %s', (ID,))
    sail = int(cur.fetchall()[0][0])
    return int(round((money / 100) * sail))


def check_time_for_pay(cur, conn, ID):
    try:
        cur.execute('SELECT time_sub FROM users WHERE id = %s', (ID,))
        result = cur.fetchall()
    except:
        conn.rollback()
        result = ''
    if not bool(len(result)):
        return False
    return result[0][0]


def set_time_sub(conn, cur, ID, time_sub):
    cur.execute('UPDATE users SET time_sub = %s WHERE id = %s', (time_sub, ID))
    conn.commit()


def pay_load(conn, cur, ID, money):
    u_money = user_money(cur, conn, ID)
    money = u_money + int(money)
    set_money(conn, cur, ID, money)
