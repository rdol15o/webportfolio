import sqlite3
import time
import urllib.request
from xml.etree import ElementTree as Et
from datetime import datetime, timedelta
from help_funcs import get_date_intervals
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

DB_PATH = os.getenv('DB_PATH')


def connect_n_close_db(func):
    def decorate(*args, **kwargs):
        try:
            con = sqlite3.connect(DB_PATH)
            con.row_factory = sqlite3.Row
            cur = con.cursor()

            func_name = func.__name__

            func(*args, **kwargs)

        except sqlite3.Error as e:
            if con:
                con.rollback()
            print('\n')
            print(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
            print(f"{func_name}(): " + str(e))
            return False
        finally:
            if con:
                con.close()

    return decorate


def download_security_properties_from_moex():
    try:
        # данные по акциям с MOEX
        moex_shares_url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.xml?iss.meta=off&iss.only=securities&securities.columns=STATUS,ISIN,SECID,SHORTNAME,SECTYPE,SECNAME,DECIMALS"
        moex_xml_shares_document = Et.fromstring(urllib.request.urlopen(moex_shares_url).read())

        # данные по ОФЗ с MOEX
        moex_bonds_url = "https://iss.moex.com/iss/engines/stock/markets/bonds/boards/TQOB/securities.xml?iss.meta=off&iss.only=securities&securities.columns=STATUS,ISIN,SECID,SHORTNAME,SECTYPE,SECNAME,DECIMALS"
        moex_xml_bonds_document = Et.fromstring(urllib.request.urlopen(moex_bonds_url).read())

        xml_rows = moex_xml_shares_document[0][0]
        for row in moex_xml_bonds_document[0][0]:
            xml_rows.append(row)

        return xml_rows if len(xml_rows) > 0 else 0

    except:
        print('\n')
        print(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        print(f"download_security_properties_from_moex(): Ошибка соединения с биржей MOEX.")
        return -1


# @connect_n_close_db
def fill_security_properties():
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        # если ни одна запись не добавится, то скажем, что обновление не выполнялось
        is_skipped = True

        xml_rows = download_security_properties_from_moex()
        if xml_rows in (-1, 0):
            return False

        for row in xml_rows:
            is_active = 1 if row.get('STATUS') == 'A' else 0
            isin = row.get('ISIN')
            ticket = row.get('SECID')
            name = row.get('SHORTNAME')
            type = row.get('SECTYPE')
            emitent = row.get('SECNAME')
            decimals = row.get('DECIMALS')

            cur.execute(
                f"select count() as count \
                from security_properties \
                where isin like '{isin}' \
                and ticket like '{ticket}'\
                and name like '{name}'\
                and type like '{type}'\
                and emitent like '{emitent}'\
                and decimals = {decimals} \
                and is_active = {is_active}")
            record = cur.fetchone()
            if record['count'] > 0:
                continue

            cur.execute(
                f"insert into security_properties \
                values(null, '{isin}', '{ticket}', '{name}', '{type}', '{emitent}', null, null, \
                        {decimals}, date('now','localtime'), null, {is_active})")

            # закрытие предыдущей записи
            cur.execute(
                f"update security_properties \
                set active_to = date('now','localtime') \
                where isin like '{isin}' \
                and active_to is null \
                and active_from != date('now','localtime')")

            is_skipped = False

        print('\n')
        print(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        if is_skipped:
            print("fill_security_properties(): Skipped")
            return True
        else:
            con.commit()
            print("fill_security_properties(): Filled")
            return True

    except sqlite3.Error as e:
        if con:
            con.rollback()
        print('\n')
        print(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        print(f"fill_security_properties({ticket}): Ошибка добавления в БД: " + str(e))
        return False
    finally:
        if con:
            con.close()


def download_security_prices_from_moex(ticket, str_date_from, str_date_to):
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        # тип бумаги
        cur.execute(
            f"select type \
            from security_properties \
            where ticket = '{ticket}' \
            and active_to is null \
            limit 1")
        record = cur.fetchone()
        type = record['type']

        # данные по ценам акции с MOEX
        if type in ('1', '2', 'D'):
            moex_url = f"https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/tqbr/securities/{ticket}.xml?iss.meta=off&iss.only=history&history.columns=SECID,OPEN,CLOSE,LOW,HIGH,TRADEDATE&from={str_date_from}&till={str_date_to}&sort_order=desc"
        # данные по ценам ОФЗ с MOEX (включает НКД)
        elif type in ('3'):
            moex_url = f"https://iss.moex.com/iss/history/engines/stock/markets/bonds/boards/tqob/securities/{ticket}.xml?iss.meta=off&iss.only=history&history.columns=SECID,OPEN,CLOSE,LOW,HIGH,ACCINT,TRADEDATE&from={str_date_from}&till={str_date_to}&sort_order=desc"

        moex_xml_document = Et.fromstring(urllib.request.urlopen(moex_url).read())
        xml_rows = moex_xml_document[0][0]

        return xml_rows if len(xml_rows) > 0 else 0

    except:
        print('\n')
        print(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        print(f"download_security_prices({ticket}, {str_date_from}, {str_date_to}): Ошибка соединения с биржей MOEX.")
        return -1
    finally:
        if con:
            con.close()


def fill_security_prices(ticket, str_date_from, str_date_to):
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        # если ни одна запись не добавится, то скажем, что добавление не выполнялось
        is_skipped = True

        cur.execute(
            f"select isin, type \
                from security_properties \
                where ticket = '{ticket}' \
                order by active_from desc \
                limit 1")
        record = cur.fetchone()
        isin = record['isin']
        type = record['type']

        # разобъем период
        dates = get_date_intervals(str_date_from, str_date_to)

        for i in range(len(dates)-1):
            xml_rows = download_security_prices_from_moex(ticket, dates[i], dates[i+1])
            if xml_rows in (-1, 0):
                continue

            for row in xml_rows:
                if row.get('OPEN') == '':
                    continue

                # для ОФЗ цену выводим из процентов
                price_open = 10 * float(row.get('OPEN')) if type in ('3') else float(row.get('OPEN'))
                price_close = 10 * float(row.get('CLOSE')) if type in ('3') else float(row.get('CLOSE'))
                price_low = 10 * float(row.get('LOW')) if type in ('3') else float(row.get('LOW'))
                price_high = 10 * float(row.get('HIGH')) if type in ('3') else float(row.get('HIGH'))
                nkd = float(row.get('ACCINT')) if type in ('3') else 'null'
                price_date = row.get('TRADEDATE')

                # если запись на дату уже есть, то пропускаем добавление
                cur.execute(
                    f"select count() as count \
                        from security_prices \
                        where isin = '{isin}' \
                        and price_date = '{price_date}'")
                record = cur.fetchone()
                if record['count'] > 0:
                    continue

                cur.execute(
                    f"insert into security_prices \
                    values(null, '{isin}', {price_open}, {price_close}, {price_low}, {price_high}, {nkd}, '{price_date}')")

                is_skipped = False

            con.commit()

        return False if is_skipped else True

    except sqlite3.Error as e:
        if con:
            con.rollback()
        print('\n')
        print(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        print(f"fill_security_prices({ticket}, {str_date_from}, {str_date_to}): Ошибка добавления в БД: " + str(e))
        return False
    finally:
        if con:
            con.close()


def fill_prices_history(str_date_from, str_date_to):
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        # если ни одна запись не добавится, то скажем, что добавление не выполнялось
        is_skipped = True

        # данные по бумагам (по имеющимся)
        cur.execute(
            f"select s.isin, s.ticket, sum(d.amount)\
                from deals d join security_properties s using(isin)\
                where s.active_to is null\
                group by s.isin, s.ticket")
        records = cur.fetchall()

        for record in range(len(records)):
            ticket = records[record]['ticket']
            is_skipped = not fill_security_prices(ticket, str_date_from, str_date_to)

        print('\n')
        print(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        if is_skipped:
            print(f"fill_prices_history({str_date_from}, {str_date_to}): Skipped")
            return True
        else:
            print(f"fill_prices_history({str_date_from}, {str_date_to}): Filled")
            return True

    except sqlite3.Error as e:
        if con:
            con.rollback()
        print('\n')
        print(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        print(f"fill_prices_history({str_date_from}, {str_date_to}): Ошибка чтения из БД: " + str(e))
        return False
    finally:
        if con:
            con.close()


def download_curr_sec_prices_from_moex():
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        # данные по текущим ценам акций с MOEX
        moex_shares_url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/.xml?iss.meta=off&iss.only=marketdata&marketdata.columns=SECID,LAST,SYSTIME"
        moex_xml_shares_document = Et.fromstring(urllib.request.urlopen(moex_shares_url).read())
        shares = moex_xml_shares_document[0][0]

        # данные по текущим ценам ОФЗ с MOEX
        moex_bonds_url = "https://iss.moex.com/iss/engines/stock/markets/bonds/boards/TQOB/securities/.xml?iss.meta=off&iss.only=marketdata&marketdata.columns=SECID,LAST,SYSTIME"
        moex_xml_bonds_document = Et.fromstring(urllib.request.urlopen(moex_bonds_url).read())
        bonds = moex_xml_bonds_document[0][0]

        # данные по НКД ОФЗ с MOEX
        moex_nkd_url = "https://iss.moex.com/iss/engines/stock/markets/bonds/boards/TQOB/securities/.xml?iss.meta=off&iss.only=securities&securities.columns=SECID,ACCRUEDINT"
        moex_xml_nkd_document = Et.fromstring(urllib.request.urlopen(moex_nkd_url).read())
        bonds_nkd = moex_xml_nkd_document[0][0]

        for bond in bonds:
            for nkd in bonds_nkd:
                if bond.get('SECID') == nkd.get('SECID'):
                    bond.set('ACCRUEDINT', nkd.get('ACCRUEDINT'))
                    break

        xml_rows = shares
        for row in bonds:
            xml_rows.append(row)

        return xml_rows if len(xml_rows) > 0 else 0

    except:
        print('\n')
        print(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        print(f"download_curr_sec_prices_from_moex(): Ошибка соединения с биржей MOEX.")
        return -1
    finally:
        if con:
            con.close()


def fill_current_security_prices():
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        # если ни одна запись не добавится, то скажем, что заполнение не выполнялось
        is_skipped = True

        # удаление предыдущих данных
        #cur.execute("delete from current_security_prices")

        xml_rows = download_curr_sec_prices_from_moex()
        if xml_rows in (-1, 0):
            return False

        # добавление новых записей по текущим ценам акций и ОФЗ с MOEX в БД
        for row in xml_rows:
            if row.get('LAST'):
                cur.execute(
                    f"select isin, type \
                    from security_properties \
                    where ticket = '{row.get('SECID')}' \
                    and active_to is null \
                    limit 1")
                record = cur.fetchone()
                isin = record['isin']
                type = record['type']

                price = 10 * float(row.get('LAST')) + float(row.get('ACCRUEDINT')) if type in ('3') else float(row.get('LAST'))
                price_date = row.get('SYSTIME')

                # если запись на дату уже есть, то пропускаем добавление
                cur.execute(
                    f"select count() as count \
                        from current_security_prices \
                        where isin = '{isin}' \
                        and price_date = '{price_date}'")
                record = cur.fetchone()
                if record['count'] > 0:
                    continue

                cur.execute(f"delete from current_security_prices where isin = '{isin}'")
                cur.execute(
                    f"insert into current_security_prices \
                    values('{isin}', {price}, '{price_date}')")

                is_skipped = False

        # последняя костыльная запись, сигнализирующая о завершении заполнения - нужна для триггера
        if not is_skipped:
            cur.execute(f"delete from current_security_prices where isin = 'last_rec'")
            cur.execute(
                f"insert into current_security_prices \
                values('last_rec', 0, datetime('now', 'localtime', '-2 hour'))")
            con.commit()

        print('\n')
        print(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        if is_skipped:
            print("fill_current_security_prices(): Skipped")
            return True
        else:
            print("fill_current_security_prices(): Filled")
            return True

    except sqlite3.Error as e:
        if con:
            con.rollback()
        print('\n')
        print(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        print("fill_current_security_prices(): Ошибка добавления в БД: " + str(e))
        return False
    finally:
        if con:
            con.close()


def start_update():
    while True:
        fill_current_security_prices()
        time.sleep(15)
