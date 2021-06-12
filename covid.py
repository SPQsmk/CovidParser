import requests
import psycopg2
import logging
import json
import time
import csv
import os
import telebot

import pandas as pd
import numpy as np

from io import StringIO
from datetime import date, timedelta
from contextlib import closing

from config import CHANNEL_NAME, config, TOKEN

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


hopkins_codes = {
    'Adygea Republic': 'RU-AD',
    'Altai Krai': 'RU-ALT',
    'Altai Republic': 'RU-AL',
    'Amur Oblast': 'RU-AMU',
    'Arkhangelsk Oblast': 'RU-ARK',
    'Astrakhan Oblast': 'RU-AST',
    'Bashkortostan Republic': 'RU-BA',
    'Belgorod Oblast': 'RU-BEL',
    'Bryansk Oblast': 'RU-BRY',
    'Buryatia Republic': 'RU-BU',
    'Chechen Republic': 'RU-CE',
    'Chelyabinsk Oblast': 'RU-CHE',
    'Chukotka Autonomous Okrug': 'RU-CHU',
    'Chuvashia Republic': 'RU-CU',
    'Dagestan Republic': 'RU-DA',
    'Ingushetia Republic': 'RU-IN',
    'Irkutsk Oblast': 'RU-IRK',
    'Ivanovo Oblast': 'RU-IVA',
    'Jewish Autonomous Okrug': 'RU-YEV',
    'Kabardino-Balkarian Republic': 'RU-KB',
    'Kaliningrad Oblast': 'RU-KGD',
    'Kalmykia Republic': 'RU-KL',
    'Kaluga Oblast': 'RU-KLU',
    'Kamchatka Krai': 'RU-KAM',
    'Karachay-Cherkess Republic': 'RU-KC',
    'Karelia Republic': 'RU-KR',
    'Kemerovo Oblast': 'RU-KEM',
    'Khabarovsk Krai': 'RU-KHA',
    'Khakassia Republic': 'RU-KK',
    'Khanty-Mansi Autonomous Okrug': 'RU-KHM',
    'Kirov Oblast': 'RU-KIR',
    'Komi Republic': 'RU-KO',
    'Kostroma Oblast': 'RU-KOS',
    'Krasnodar Krai': 'RU-KDA',
    'Krasnoyarsk Krai': 'RU-KYA',
    'Kurgan Oblast': 'RU-KGN',
    'Kursk Oblast': 'RU-KRS',
    'Leningrad Oblast': 'RU-LEN',
    'Lipetsk Oblast': 'RU-LIP',
    'Magadan Oblast': 'RU-MAG',
    'Mari El Republic': 'RU-ME',
    'Mordovia Republic': 'RU-MO',
    'Moscow': 'RU-MOW',
    'Moscow Oblast': 'RU-MOS',
    'Murmansk Oblast': 'RU-MUR',
    'Nenets Autonomous Okrug': 'RU-NEN',
    'Nizhny Novgorod Oblast': 'RU-NIZ',
    'North Ossetia - Alania Republic': 'RU-SE',
    'Novgorod Oblast': 'RU-NGR',
    'Novosibirsk Oblast': 'RU-NVS',
    'Omsk Oblast': 'RU-OMS',
    'Orel Oblast': 'RU-ORL',
    'Orenburg Oblast': 'RU-ORE',
    'Penza Oblast': 'RU-PNZ',
    'Perm Krai': 'RU-PER',
    'Primorsky Krai': 'RU-PRI',
    'Pskov Oblast': 'RU-PSK',
    'Rostov Oblast': 'RU-ROS',
    'Ryazan Oblast': 'RU-RYA',
    'Saint Petersburg': 'RU-SPE',
    'Sakha (Yakutiya) Republic': 'RU-SA',
    'Sakhalin Oblast': 'RU-SAK',
    'Samara Oblast': 'RU-SAM',
    'Saratov Oblast': 'RU-SAR',
    'Smolensk Oblast': 'RU-SMO',
    'Stavropol Krai': 'RU-STA',
    'Sverdlovsk Oblast': 'RU-SVE',
    'Tambov Oblast': 'RU-TAM',
    'Tatarstan Republic': 'RU-TA',
    'Tomsk Oblast': 'RU-TOM',
    'Tula Oblast': 'RU-TUL',
    'Tver Oblast': 'RU-TVE',
    'Tyumen Oblast': 'RU-TYU',
    'Tyva Republic': 'RU-TY',
    'Udmurt Republic': 'RU-UD',
    'Ulyanovsk Oblast': 'RU-ULY',
    'Vladimir Oblast': 'RU-VLA',
    'Volgograd Oblast': 'RU-VGG',
    'Vologda Oblast': 'RU-VLG',
    'Voronezh Oblast': 'RU-VOR',
    'Yamalo-Nenets Autonomous Okrug': 'RU-YAN',
    'Yaroslavl Oblast': 'RU-YAR',
    'Zabaykalsky Krai': 'RU-ZAB',
    'Crimea Republic*': 'RU-CR',
    'Sevastopol*': 'RU-SEV'
}


rf_codes = {
    'Москва': 'RU-MOW',
    'Санкт-Петербург': 'RU-SPE',
    'Московская область': 'RU-MOS',
    'Нижегородская область': 'RU-NIZ',
    'Ростовская область': 'RU-ROS',
    'Свердловская область': 'RU-SVE',
    'Воронежская область': 'RU-VOR',
    'Красноярский край': 'RU-KYA',
    'Иркутская область': 'RU-IRK',
    'Самарская область': 'RU-SAM',
    'Архангельская область': 'RU-ARK',
    'Саратовская область': 'RU-SAR',
    'Челябинская область': 'RU-CHE',
    'Волгоградская область': 'RU-VGG',
    'Ханты-Мансийский АО': 'RU-KHM',
    'Пермский край': 'RU-PER',
    'Ульяновская область': 'RU-ULY',
    'Ставропольский край': 'RU-STA',
    'Хабаровский край': 'RU-KHA',
    'Алтайский край': 'RU-ALT',
    'Мурманская область': 'RU-MUR',
    'Краснодарский край': 'RU-KDA',
    'Омская область': 'RU-OMS',
    'Республика Карелия': 'RU-KR',
    'Пензенская область': 'RU-PNZ',
    'Вологодская область': 'RU-VLG',
    'Ленинградская область': 'RU-LEN',
    'Приморский край': 'RU-PRI',
    'Республика Коми': 'RU-KO',
    'Оренбургская область': 'RU-ORE',
    'Кировская область': 'RU-KIR',
    'Забайкальский край': 'RU-ZAB',
    'Новосибирская область': 'RU-NVS',
    'Республика Крым': 'RU-CR',
    'Тверская область': 'RU-TVE',
    'Ярославская область': 'RU-YAR',
    'Тульская область': 'RU-TUL',
    'Брянская область': 'RU-BRY',
    'Ямало-Ненецкий автономный округ': 'RU-YAN',
    'Белгородская область': 'RU-BEL',
    'Курская область': 'RU-KRS',
    'Республика Бурятия': 'RU-BU',
    'Псковская область': 'RU-PSK',
    'Республика Саха (Якутия)': 'RU-SA',
    'Республика Башкортостан': 'RU-BA',
    'Кемеровская область': 'RU-KEM',
    'Тюменская область': 'RU-TYU',
    'Ивановская область': 'RU-IVA',
    'Калужская область': 'RU-KLU',
    'Орловская область': 'RU-ORL',
    'Астраханская область': 'RU-AST',
    'Удмуртская Республика': 'RU-UD',
    'Республика Дагестан': 'RU-DA',
    'Владимирская область': 'RU-VLA',
    'Калининградская область': 'RU-KGD',
    'Томская область': 'RU-TOM',
    'Тамбовская область': 'RU-TAM',
    'Липецкая область': 'RU-LIP',
    'Новгородская область': 'RU-NGR',
    'Смоленская область': 'RU-SMO',
    'Рязанская область': 'RU-RYA',
    'Республика Чувашия': 'RU-CU',
    'Кабардино-Балкарская Республика': 'RU-KB',
    'Республика Хакасия': 'RU-KK',
    'Сахалинская область': 'RU-SAK',
    'Амурская область': 'RU-AMU',
    'Костромская область': 'RU-KOS',
    'Республика Татарстан': 'RU-TA',
    'Курганская область': 'RU-KGN',
    'Республика Калмыкия': 'RU-KL',
    'Республика Мордовия': 'RU-MO',
    'Карачаево-Черкесская Республика': 'RU-KC',
    'Республика Северная Осетия — Алания': 'RU-SE',
    'Республика Алтай': 'RU-AL',
    'Республика Тыва': 'RU-TY',
    'Республика Ингушетия': 'RU-IN',
    'Камчатский край': 'RU-KAM',
    'Республика Адыгея': 'RU-AD',
    'Севастополь': 'RU-SEV',
    'Республика Марий Эл': 'RU-ME',
    'Чеченская Республика': 'RU-CE',
    'Магаданская область': 'RU-MAG',
    'Еврейская автономная область': 'RU-YEV',
    'Ненецкий автономный округ': 'RU-NEN',
    'Чукотский автономный округ': 'RU-CHU'
}


logging.basicConfig(level=logging.INFO, filename='logs/app.log', format='%(asctime)s - %(levelname)s ----- %(message)s', datefmt='%d-%b-%y %H:%M:%S')


class CovidParser:
    def __init__(self):
        self.timeout = 600

        self.coronavirusrf_url = r'https://xn--80aesfpebagmfblc0a.xn--p1ai/information/'
        self.hopkins_url = r'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'

        self.path = os.path.join(os.getcwd(), 'logs/covid.json')
        self.config = config

        self.bot = telebot.TeleBot(TOKEN)
        self.CHANNEL_NAME = CHANNEL_NAME

    def get_hopkins_data(self, _date):
        url = self.hopkins_url + _date.strftime('%m-%d-%Y') + '.csv'

        try:
            r = requests.get(url)
            r.raise_for_status()
            return r.text
        except (Exception, requests.exceptions.RequestException) as error:
            logging.exception(f'Exception occured. Date: "{_date}"')
            raise error

    def add_regs(self):
        logging.info('Addind regions started')

        try:
            with closing(psycopg2.connect(**self.config)) as con:
                with con.cursor() as cur:
                    con.autocommit = True
                    for name in rf_codes:
                        cur.execute('SELECT check_reg(%s)', (rf_codes[name],))
                        if cur.fetchone()[0] is None:
                            cur.execute('CALL add_reg(%s,%s)', (name, rf_codes[name]))
        except (Exception, psycopg2.Error) as error:
            logging.exception('Exception occured')
            raise error

        logging.info('Addind regions ended')

    def add_summary_data(self, cur, code, day, inf, rec, dec):
        cur.execute('SELECT check_summary_data(%s, %s)', (code, day))
        if cur.fetchone()[0] is None:
            cur.execute('CALL add_summary_data(%s,%s,%s,%s,%s)', (code, day, inf, rec, dec))

    def add_date(self, cur, day):
        cur.execute('SELECT check_date(%s)', (day,))
        if cur.fetchone()[0] is None:
            cur.execute('CALL add_date(%s)', (day,))

    def append_data(self, data, code, inf, rec, dec):
        data.append({
            'code': code,
            'infected': int(inf),
            'recovered': int(rec),
            'deceased': int(dec)
        })

    def parse_coronavirusrf(self):
        logging.info('Parsing "coronavirus.rf" started')

        driver = webdriver.Chrome()
        driver.get(self.coronavirusrf_url)

        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CLASS_NAME, 'cv-section__title')))
        update_date = driver.find_element_by_class_name('cv-section__title').find_element_by_tag_name('small').text

        _date = date.today()
        if (_date - timedelta(days=1)).day == int(update_date.split()[3]):
            _date -= timedelta(days=1)

        data = {
            'date': _date.strftime('%Y-%m-%d'),
            'regions': []
        }

        for i in range(1, len(driver.find_elements_by_class_name('col-region'))):
            driver.execute_script(f"document.getElementsByClassName('col-region')[{str(i)}].click()")

            WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CLASS_NAME, 'cv-popup__title')))
            name = driver.find_element_by_class_name('cv-popup__title').text

            WebDriverWait(driver, 10).until(EC.visibility_of_all_elements_located((By.CLASS_NAME, 'cv-stats-region__item-value')))
            result = driver.find_elements_by_class_name('cv-stats-region__item-value')

            inf = result[0].text.replace(' ', '')
            rec = result[2].text.replace(' ', '')
            dec = result[4].text.replace(' ', '')
            self.append_data(data['regions'], rf_codes[name], inf, rec, dec)

        driver.close()

        logging.info('Parsing "coronavirus.rf" ended')

        return data

    def csv_to_json(self, _date, csv_data):
        data = {
            'date': _date.strftime('%Y-%m-%d'),
            'regions': []
        }

        for row in csv.reader(StringIO(csv_data)):
            if (row[3] == 'Russia') or (row[2] in ['Crimea Republic*', 'Sevastopol*']):
                self.append_data(data['regions'], hopkins_codes[row[2]], row[7], row[9], row[8])

        return data

    def write_json(self, data):
        try:
            with open(self.path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                logging.info(f'Writed "{self.path}" file')
        except Exception as error:
            logging.exception('Exception occured')
            raise error

    def write_data_to_DB(self, data):
        try:
            with closing(psycopg2.connect(**self.config)) as con:
                with con.cursor() as cur:
                    con.autocommit = True
                    self.add_date(cur, data['date'])
                    for row in data['regions']:
                        self.add_summary_data(cur, row['code'], data['date'], row['infected'], row['recovered'], row['deceased'])
        except (Exception, psycopg2.Error) as error:
            logging.exception('Exception occured')
            raise error

    def get_last_date(self):
        try:
            with closing(psycopg2.connect(**self.config)) as con:
                with con.cursor() as cur:
                    cur.execute('SELECT get_last_summary_date()')
                    last_date = cur.fetchone()[0]
        except (Exception, psycopg2.Error) as error:
            logging.exception('Exception occured')
            raise error

        if last_date is None:
            last_date = date(2020, 6, 1)
        else:
            last_date -= timedelta(days=1)

        return last_date

    def parse_hopkins(self):
        logging.info('Parsing "Hopkins" started')

        prev_date = self.get_last_date()
        end_date = date.today()

        logging.info(f'Start DATE: "{prev_date}"')

        while prev_date < end_date:
            try:
                data = self.get_hopkins_data(prev_date)
            except (Exception, requests.exceptions.RequestException) as error:
                if prev_date == date.today() - timedelta(days=1):
                    logging.warning(f'404 Error. DATE: "{prev_date}"')
                    break
                logging.exception('Exception occured')
                raise error

            res = self.csv_to_json(prev_date, data)
            prev_date += timedelta(days=1)
            self.write_data_to_DB(res)

        logging.info(f'Last added data. Date: "{res["date"]}"')
        logging.info('Parsing "Hopkins" ended')

        if not os.path.exists(self.path):
            self.write_json(res)
        else:
            with open(self.path) as f:
                existing_data = json.load(f)

            if date.fromisoformat(existing_data['date']) < date.fromisoformat(res['date']):
                self.write_json(res)

    def equal_data(self, prev_data, next_data):
        data1 = sorted(prev_data['regions'], key=lambda x: x['code'])
        data2 = sorted(next_data['regions'], key=lambda x: x['code'])

        return data1 == data2

    def create_time_series_files(self):
        logging.info('Started creating files with time series')

        try:
            with closing(psycopg2.connect(**self.config)) as con:
                with con.cursor() as cur:
                    cur.execute('SELECT * from get_summary_data()')
                    data = cur.fetchall()
        except (Exception, psycopg2.Error) as error:
            logging.exception('Exception occured')
            raise error

        start_date = data[0][1]
        current_date = start_date

        result = {
            'Регион': [],
            'Случай': [],
            current_date.strftime('%m-%d-%Y'): []
        }

        for row in data:
            if row[1] == start_date:
                for case in ['Заболело', 'Выздоровело', 'Умерло']:
                    result['Регион'].append(row[0])
                    result['Случай'].append(case)
            if row[1] != current_date:
                current_date = row[1]
                result[current_date.strftime('%m-%d-%Y')] = []
            for i in range(2, 5):
                result[current_date.strftime('%m-%d-%Y')].append(row[i])

        df_summary = pd.DataFrame(result)

        df_daily = pd.DataFrame(np.diff(df_summary.iloc[:, 2:]), columns=df_summary.columns[3:])
        df_daily.insert(0, 'Регион', df_summary['Регион'])
        df_daily.insert(1, 'Случай', df_summary['Случай'])

        df_summary.to_excel('logs/covid_summary.xlsx', sheet_name='Коронавирус', index=False)
        df_summary.to_csv('logs/covid_summary.csv', index=False)
        df_daily.to_excel('logs/covid_daily.xlsx', sheet_name='Коронавирус', index=False)
        df_daily.to_csv('logs/covid_daily.csv', index=False)

        logging.info('Ended creating files with time series')

    def post_data(self, _date):
        self.bot.send_message(self.CHANNEL_NAME, f'Актуальная информация на: "{_date}"')
        for filename in ['logs/covid_daily.csv', 'logs/covid_summary.csv', 'logs/covid_daily.xlsx', 'logs/covid_summary.xlsx']:
            with open(filename, 'rb') as f:
                self.bot.send_document(self.CHANNEL_NAME, f)
        logging.info(f'Files have been posted')

    def start_parsing(self):
        self.add_regs()
        self.parse_hopkins()

        while True:
            next_data = self.parse_coronavirusrf()

            if len(next_data['regions']) != 85:
                logging.info(f'Data not fully posted')
                time.sleep(self.timeout)
                continue

            if not os.path.exists(self.path):
                self.write_json(next_data)

            with open(self.path) as f:
                prev_data = json.load(f)

            logging.info(f'Collected infromation from "{self.path}" file')

            if not self.equal_data(prev_data, next_data):
                self.write_data_to_DB(next_data)
                logging.info(f'Added data. Date: "{next_data["date"]}"')
                self.create_time_series_files()
                self.post_data(next_data['date'])
                self.write_json(next_data)
            else:
                logging.info('The data is up-to date')

            time.sleep(self.timeout)


if __name__ == '__main__':
    cv = CovidParser()
    cv.start_parsing()
