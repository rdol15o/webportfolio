import datetime
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
            f'select a.acc_name || " - активен с " || ulu.active_from || " до " || ifnull(ulu.active_to, "-") as acc \
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


def get_cash_db(tgm_user_id, for_date='null'):
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        cur.execute(
            f'select a.acc_name || ": " || t.sum || " ₽" as sum\
                from (select ulu.acc_id, round(sum(act.value), 2) as sum\
                        from users_link_telegram_users utu\
                                join users u using(user_id)\
                                join users_link_accounts ulu using(user_id)\
                                join actions act using(acc_id)\
                        where utu.tgm_user_id = {tgm_user_id}\
                        and utu.active_to is null\
                        and u.active_to is null\
                        and act.act_date  <= ifnull({for_date}, current_date)\
                        group by ulu.acc_id) as t\
                    join accounts a using(acc_id)\
                order by a.acc_id')

        records = cur.fetchall()
        cash = '\n'.join([rec['sum'] for rec in records])
        return cash

    except sqlite3.Error as e:
        print(str(e))
    finally:
        if con:
            con.close()


def get_refills_db(tgm_user_id):
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        cur.execute(
            f'select t.year|| " г. " || a.acc_name || ": " || t.sum || " ₽" as sum\
                from (select ulu.acc_id, strftime("%Y", act.act_date) as year, round(sum(act.value), 2) as sum\
                        from users_link_telegram_users utu\
                                join users u using(user_id)\
                                join users_link_accounts ulu using(user_id)\
                                join actions act using(acc_id)\
                        where utu.tgm_user_id = {tgm_user_id}\
                        and utu.active_to is null\
                        and u.active_to is null\
                        and act.act_type_id in (1, 2)\
                        group by ulu.acc_id, strftime("%Y", act.act_date)) as t\
                    join accounts a using(acc_id)\
                UNION ALL\
                select "Всего: " || round(sum(act.value), 2) || " ₽" as sum\
                from users_link_telegram_users utu\
                        join users u using(user_id)\
                        join users_link_accounts ulu using(user_id)\
                        join actions act using(acc_id)\
                where utu.tgm_user_id = {tgm_user_id}\
                and utu.active_to is null\
                and u.active_to is null\
                and act.act_type_id in (1, 2)')

        records = cur.fetchall()
        refills = '\n'.join([rec['sum'] for rec in records])
        return refills

    except sqlite3.Error as e:
        print(str(e))
    finally:
        if con:
            con.close()


def get_analytics_db(con, tgm_user_id, for_date='null', ticket='null'):
    try:
        cursor = con.cursor()

        cursor.execute(f'select user_id\
                    from users_link_telegram_users utu\
                    join users u using(user_id)\
                    where tgm_user_id = {tgm_user_id}\
                    and utu.active_to is null\
                    and u.active_to is null')

        user_id = cursor.fetchone()['user_id']

        cursor.execute(
            f'with sec_hist as (select user_id, isin, ticket, ifnull({for_date}, current_date) as sec_date, balance_amount\
                                                ,case when balance_amount!=0\
                                                    then round(balance_cost, 2)\
                                                    else 0\
                                                    end as balance_cost\
                                                ,round(ifnull(balance_cost/balance_amount, 0)\
                                                                , (select decimals from security_properties where active_to is null and isin = sec.isin)) as avg_price_rounded\
                                                ,row_number() over (partition by user_id, ticket order by deal_date desc) as num\
                                        from (select ula.user_id, d.isin, sp.ticket, d.deal_date\
                                                    ,sum(sum(d.amount)) over (partition by ula.user_id, d.isin order by d.deal_date) as balance_amount\
                                                    ,sum( /*сумма по дням*/\
                                                        sum( /*сумма внутри дня*/\
                                                            d.amount*d.price - case when d.deal_type = "sell"\
                                                                                    then d.amount* (d.price - (select avg_price\
                                                                                                                from securities_history\
                                                                                                                where acc_id = d.acc_id\
                                                                                                                and isin = d.isin\
                                                                                                                and deal_type in ("buy", "swap")\
                                                                                                                and balance_amount > 0\
                                                                                                                and sec_date < d.deal_date\
                                                                                                                order by sec_date desc\
                                                                                                                limit 1)\
                                                                                                    )\
                                                                                    else 0\
                                                                                    end\
                                                            )\
                                                        ) 	over (partition by ula.user_id, d.isin order by d.deal_date) as balance_cost\
                                                from deals d join security_properties sp using(isin)\
                                                     join users_link_accounts ula using(acc_id)\
                                                where sp.active_to is null\
                                                and d.deal_date <= ifnull({for_date}, current_date)\
                                                and ula.user_id = {user_id}\
                                                and (ticket = {ticket} or {ticket} is null)\
                                                group by ula.user_id, d.isin, sp.ticket, d.deal_type, d.deal_date) sec)\
                    ,current_prices as (select pr.*\
                                                ,row_number() over (partition by isin order by price_date desc) as num\
                                        from (select isin, price_close + ifnull(nkd, 0) as curr_price, price_date\
                                                from security_prices p\
                                                where price_date <= current_date\
                                                and price_date >= date("now", "-20 day")\
                                                union all\
                                                select * from current_security_prices\
                                                where price_date <= datetime("now", "localtime")) pr)\
                    ,yesterday_prices as (select pr.*\
                                                ,row_number() over(partition by isin order by price_date desc) as num\
                                            from (select isin, price_close + ifnull(nkd, 0) as yest_price, price_date\
                                                    from security_prices p\
                                                    where price_date <= date(current_date, "-1 day")\
                                                    and price_date >= date("now", "-60 day")) pr)\
                    select ticket, avg_price_rounded\
                          ,curr_price\
                          ,balance_cost\
                          ,round(balance_amount*curr_price, 2) as curr_cost\
                          ,round(balance_amount*curr_price - balance_cost, 2) as sec_profit\
                          ,round(100 * (balance_amount*curr_price - balance_cost) / balance_cost, 2) as sec_profit_perc\
                          ,sum(balance_cost) over (partition by user_id) as sum_cost\
                          ,sum(round(balance_amount*curr_price, 2)) over (partition by user_id) as curr_sum_cost\
                          ,sum(round(balance_amount*curr_price - balance_cost, 2)) over (partition by user_id) as sum_profit\
                          ,round(100 * sum(balance_amount*curr_price - balance_cost) over (partition by user_id) / sum(balance_cost) over (partition by user_id), 2) as sum_profit_perc\
                          ,yest_price\
                          ,round(balance_amount * yest_price, 2) as yest_cost\
                          ,round(balance_amount * curr_price - balance_amount * yest_price, 2) as daily_sec_profit\
                          ,round(100 * (balance_amount * curr_price - balance_amount * yest_price) / (balance_amount * yest_price), 2) as daily_sec_profit_perc\
                          ,sum(round(balance_amount * yest_price, 2)) over(partition by user_id) as yest_sum_cost\
                          ,sum(round(balance_amount * curr_price - balance_amount * yest_price, 2)) over(partition by user_id) as daily_sum_profit\
                          ,round(100 * sum(balance_amount * curr_price - balance_amount * yest_price) over(partition by user_id) / sum(balance_amount * yest_price) over(partition by user_id), 2) as daily_sum_profit_perc\
                    from sec_hist sh, current_prices pr, yesterday_prices yest_pr\
                    where sh.isin = pr.isin\
                    and sh.isin = yest_pr.isin\
                    and sh.num = 1\
                    and pr.num = 1\
                    and yest_pr.num = 1\
                    and balance_amount != 0\
                    order by sec_profit_perc desc')

        return cursor

    except sqlite3.Error as e:
        print(str(e))


def get_analytics_total_db(tgm_user_id, is_simple=True):
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row

        cursor = get_analytics_db(con=con, tgm_user_id=tgm_user_id)
        records = cursor.fetchall()

        sum_cost = round(records[0]['sum_cost'], 2)
        curr_sum_cost = round(records[0]['curr_sum_cost'], 2)
        sum_profit = round(records[0]['sum_profit'], 2)
        sum_profit_perc = records[0]['sum_profit_perc']

        if is_simple:
            tickets = '\n'.join([rec['ticket'] + ' (' + str(rec['sec_profit_perc']) + '%)' for rec in records])
        else:
            tickets = '\n\n'.join([rec['ticket']
                                   + ' (' + str(rec['sec_profit_perc']) + '%)'
                                   + '\nцена: ' + str(rec['avg_price_rounded']) + ' -> ' + str(rec['curr_price'])
                                   + '\nстоимость: ' + str(rec['balance_cost']) + ' -> ' + str(rec['curr_cost'])

                                   for rec in records])

        return tickets + '\n\n----' + '\nИтого: ' + str(sum_cost) + ' -> ' + str(curr_sum_cost) + '\n\t\t\t\t\t\t' + ' (' + str(sum_profit) + ')' + ' (' + str(sum_profit_perc) + '%)'

    except sqlite3.Error as e:
        print(str(e))
    finally:
        if con:
            con.close()


def get_analytics_daily_db(tgm_user_id, is_simple=True):
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row

        cursor = get_analytics_db(con=con, tgm_user_id=tgm_user_id)
        records = cursor.fetchall()

        yest_sum_cost = round(records[0]['yest_sum_cost'], 2)
        curr_sum_cost = round(records[0]['curr_sum_cost'], 2)
        daily_sum_profit = round(records[0]['daily_sum_profit'], 2)
        daily_sum_profit_perc = records[0]['daily_sum_profit_perc']

        if is_simple:
            tickets = '\n'.join([rec['ticket'] + ' (' + str(rec['daily_sec_profit_perc']) + '%)' for rec in records])

        return tickets + '\n\n----' + '\nИтого: ' + str(yest_sum_cost) + ' -> ' + str(curr_sum_cost) + '\n\t\t\t\t\t\t' + ' (' + str(daily_sum_profit) + ')' + ' (' + str(daily_sum_profit_perc) + '%)'

    except sqlite3.Error as e:
        print(str(e))
    finally:
        if con:
            con.close()