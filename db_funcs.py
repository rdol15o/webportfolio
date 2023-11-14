import sqlite3
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

DB_PATH = os.getenv('DB_PATH')

def add_user_db(tgm_user_id, tgm_user_name):
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        cur.execute(
            f'select count(tgm_user_id) as count \
            from telegram_users \
            where tgm_user_id = {tgm_user_id} \
            and active_to is null')
        record = cur.fetchone()
        if record['count'] > 0:
            print(f'add_user_db({tgm_user_id}, {tgm_user_name}): телеграм-пользователь уже существует в БД')
            return

        cur.execute(
            f'insert into telegram_users \
            values({tgm_user_id}, {tgm_user_name}, datetime("now"), null)')

        cur.execute(
            f'insert into users \
            values(null, {tgm_user_name}, null, null, datetime("now"), null) \
            returning user_id')
        record = cur.fetchone()
        user_id = record['user_id']

        cur.execute(
            f'insert into users_link_telegram_users \
            values(null, {user_id}, {tgm_user_id}, datetime("now"), null)')

        con.commit()

        print(f'add_user_db({tgm_user_id}, {tgm_user_name}): новый телеграм-пользователь связан с пользователем {user_id}')

    except sqlite3.Error as e:
        if con:
            con.rollback()
        print(str(e))
    finally:
        if con:
            con.close()


def get_accounts_db(tgm_user_id):
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        cur.execute(
            f'select a.acc_name || " - активен с " || ulu.active_from || " до " || ifnull(ulu.active_to, "") as acc \
            from users_link_telegram_users utu \
                    join users u using(user_id) \
                    join users_link_accounts ulu using(user_id) \
                    join accounts a using(acc_id) \
            where utu.tgm_user_id = {tgm_user_id} \
            and utu.active_to is null \
            and u.active_to is null \
            order by a.acc_id')

        records = cur.fetchall()
        accounts = '\n'.join([rec['acc'] for rec in records])
        return accounts

    except sqlite3.Error as e:
        print(str(e))
    finally:
        if con:
            con.close()


def get_deals_db(tgm_user_id, start=0):
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        cur.execute(
            f'select (row_number() over (order by d.deal_date desc) + {start}) || ". " || \
                      a.acc_name || "\n   " || sp.ticket || "\n   " || d.deal_date || "\n   " || \
                      d.amount || " шт " || d.price || " руб" as deal \
            from users_link_telegram_users utu \
                    join users u using(user_id) \
                    join users_link_accounts ulu using(user_id) \
                    join accounts a using(acc_id) \
                    join deals d using(acc_id) \
                    join security_properties sp using(isin) \
            where utu.tgm_user_id = {tgm_user_id} \
            and utu.active_to is null \
            and u.active_to is null \
            and sp.active_to is null \
            order by d.deal_date desc \
            limit 10 offset {start}')

        records = cur.fetchall()
        deals = '\n'.join([rec['deal'] for rec in records])
        return deals

    except sqlite3.Error as e:
        print(str(e))
    finally:
        if con:
            con.close()