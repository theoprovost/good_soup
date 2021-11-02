#!pip install beautifulsoup4
from bs4 import BeautifulSoup
from os.path import abspath
import pandas as pd
import re
import requests
from datetime import datetime
import glob
import os
import psycopg2 as pgconn
import urllib.parse as urlparse
import subprocess


class Scrapper(object):
    def __init__(self, base_url: str = 'https://www.imdb.com/search/title/', url_params: dict = {}):
        self.BASE_URL = base_url
        self.URL_PARAMS = url_params

        self.URL = self.construct_url()
        self.DATA_PATH = abspath('./data')

        self.DATA_FIELDS = ['IMBd_id', 'title', 'ranking', 'ranking_name', 'released_year',
                            'duration_min', 'genre', 'collected_at']

        self.DB = self.DB(self.DATA_FIELDS, self.DATA_PATH)

    def __del__(self) -> None:
        return None

    def crawl(self):
        '''Main function :
            - itter over pages
            - make soup + extract data
            - store data as csv
        '''
        soup = self.make_soup(start=0, count=250)
        data = self.extract_info(soup.body)
        self.store_csv(data)

    def store_csv(self, data):
        '''Method to store data as .csv'''
        file_path = self.DATA_PATH + '/' + str(datetime.now()) + '.csv'
        pd.DataFrame(data, columns=self.DATA_FIELDS).to_csv(
            file_path, sep=',', index=False)

    def concat_csv(self):
        '''Gets all the file in folder, concat them into a new one, then remove files'''
        filenames = [i for i in glob.glob(f'{self.DATA_PATH}/*.csv')]
        combined_csv = pd.concat([pd.read_csv(f) for f in filenames])

        outdir = self.DATA_PATH + '/' + str(datetime.now())
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        combined_csv.to_csv(f'{outdir}/data.csv',
                            index=False, encoding='utf-8-sig')

        for f in filenames:
            os.remove(f)

    def get_n(self):
        '''Retrieve elements n :
            - n_start : first n
            - n_res : n of results on page
            - n_end : total n results
        '''
        soup = self.make_soup()
        desc = soup.find(class_='desc').span.text
        n_start = desc[0]
        n_res = desc[2:4]
        n_end = [int(s) for s in desc.split() if s.isdigit()][0]
        return (int(n_start), int(n_res), int(n_end))

    def make_soup(self, start: int = 0, count: int = 100):
        '''Concat url with start then call fetch. Retrieve BeautifulSoup html parser'''
        url = self.URL + '&start=' + str(start) + '&count=' + str(count)
        print('Fetching : ', url)
        html = self.fetch(url)
        return BeautifulSoup(html, "html.parser")

    def extract_info(self, soup) -> list:
        '''Method responsible to extract desired data from a soup parser'''
        tmp_data = []
        films = soup.find_all(class_='lister-item mode-advanced')
        for film in films:
            # Select method is more restrictive

            img_div, title, * \
                rest = film.find_all('a', {'href': re.compile(r'^/title/')})

            tmp_data.append([
                title.get('href').split('/')[2],  # IMBd_id
                title.text,
                self.sanitize_text(film.select(
                    "span.lister-item-index")[0].text),  # ranking for
                'top_250',
                self.sanitize_text(
                    film.find('span', {'class': 'lister-item-year'}).text),
                self.sanitize_text(
                    film.find('span', {'class': 'runtime'}).text),
                self.sanitize_text(film.find('span', {'class': 'genre'}).text),
                datetime.now()
            ])

        return tmp_data

    def sanitize_text(self, txt: str) -> str:
        '''Remove unwanted chars in str'''
        char_list = [' ', '\(', '\)', '\n', '\.']
        return re.sub('|'.join(char_list), '', txt)

    def construct_url(self) -> str:
        '''Construct the URL with the given url params'''
        url = self.BASE_URL
        for i, x in enumerate(self.URL_PARAMS.items()):
            if (i == 0 and self.BASE_URL[-1] != '?'):
                url += '?'

            url += x[0] + '=' + str(x[1]) + '&'
            if (i == len(self.URL_PARAMS) - 1):
                url = url[:-1]

        return url

    def fetch(self, url: str = None) -> str or bool:
        '''Fetch the data via requests package'''
        if url is None:
            url = self.URL

        # Change headers accordingly if result is needed in FR
        req = requests.get(url, headers={"Accept-Language": "en-US, en;q=0.5"}
                           )
        status = req.status_code
        if str(status).startswith('2'):
            return req.text
        else:
            return False

    class DB():
        def __init__(self, db_fields: str, data_path: str) -> None:
            self.DB_CONNECTION_STR = 'postgresql://postgres:@localhost:5432/good_soup'
            self.DB_SCRIPTS_PATH = abspath('./database')
            self.DB_FIELDS = db_fields
            self.DB_USER, self.DB_PWD, self.DB_NAME, self.DB_HOST = self.decompose_db_connection_str()

            self.DATA_PATH = data_path

        def connect(self):
            try:
                conn = pgconn.connect(
                    host=self.DB_HOST,
                    database=self.DB_NAME,
                    user=self.DB_USER,
                    password=self.DB_PWD
                )
            except (Exception, pgconn.DatabaseError) as error:
                print('⚠︎ Postgres connection error', error)
                conn = False
            finally:
                print('Connection established with postgres.')
                conn.autocommit = True
                return conn

        def decompose_db_connection_str(self) -> tuple:
            url = urlparse.urlparse(self.DB_CONNECTION_STR)
            return (url.username, url.password, url.path[1:], url.hostname)

        def get_newest_data_path(self) -> str:
            data_path = abspath(self.DATA_PATH)
            lst = glob.glob(data_path + '/*.csv')
            lst.sort(reverse=True)
            return lst[0]

        def gen_createdb_script(self) -> None:
            sql_txt = f'DROP DATABASE IF EXISTS {self.DB_NAME} WITH (FORCE);\n'
            sql_txt += f'CREATE DATABASE {self.DB_NAME};'

            file_path = self.DB_SCRIPTS_PATH + '/create_db.sql'
            with open(file_path, 'w') as f:
                f.write(sql_txt)

        def gen_createtable_script(self) -> None:
            sql_txt = f'DROP TABLE IF EXISTS records;'
            sql_txt += f'\n\n CREATE TABLE records ('
            for x in self.DB_FIELDS:
                sql_txt += f'\n\t"{x}" TEXT,'

            last_comma = sql_txt.rfind(",")
            sql_txt = sql_txt[:last_comma] + "" + sql_txt[last_comma+1:]
            sql_txt += '\n);'

            file_path = self.DB_SCRIPTS_PATH + '/create_table.sql'
            with open(file_path, 'w') as f:
                f.write(sql_txt)

        def gen_seed_script(self) -> None:
            newest_data_path = self.get_newest_data_path()
            sql_txt = f'COPY records FROM \'{newest_data_path}\'\n'
            sql_txt += "(DELIMITER ',',\nnull \'\',\nFORMAT CSV,\nHEADER);\n"
            file_path = self.DB_SCRIPTS_PATH + '/seed.sql'
            with open(file_path, 'w') as f:
                f.write(sql_txt)

        def execute_scripts(self) -> None:
            self.gen_createdb_script()
            self.gen_createtable_script()
            self.gen_seed_script()

            create_db_script = abspath(self.DB_SCRIPTS_PATH + '/create_db.sql')
            create_db_cmd = f'psql postgres -f {create_db_script}'
            process = subprocess.Popen(
                create_db_cmd.split(), stdout=subprocess.PIPE)
            process.communicate()
            print(f'PSQL executing : {create_db_script}')
            print('Restarting connection with postgres.')
            self.cur = self.connect().cursor()

            create_table_script = abspath(
                self.DB_SCRIPTS_PATH + '/create_table.sql')
            seed_script = abspath(self.DB_SCRIPTS_PATH + '/seed.sql')

            for path in [create_table_script, seed_script]:
                with open(path, 'r') as f:
                    print(f'PSQL executing : {path}')
                    script = f.read()
                    self.cur.execute(script)


###############################
URL_PARAMS = {
    'groups': 'top_250',
    'sort': 'user_rating,desc'
}

scrapper = Scrapper(url_params=URL_PARAMS)
scrapper.crawl()
scrapper.DB.execute_scripts()

'''
- diff studios
- diff en fn des années
- score en fn de la durée
- corrs


- les gros films americains
- series
'''
