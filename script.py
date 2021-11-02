#!pip install beautifulsoup4
from bs4 import BeautifulSoup
from os.path import abspath
import pandas as pd
import re
import requests
from datetime import datetime
import glob
import os


class Scrapper(object):
    def __init__(self, base_url: str = 'https://www.imdb.com/search/title/', url_params: dict = {}):
        self.BASE_URL = base_url
        self.URL_PARAMS = url_params

        self.URL = self.construct_url()
        self.DATA_PATH = abspath('./data')

        self.DATA_FIELDS = ['IMBd_id', 'title', 'released_year',
                            'duration_min', 'genre', 'collected_at']

    def __del__(self) -> None:
        return False

    def crawl(self):
        n_start, n_res, n_end = self.get_n()
        n_page = round(n_end / n_res)
        for i in range(n_page):
            if i == 0:
                n = 1
            else:
                n += n_res

            soup = self.make_soup(start=n)
            data = self.extract_info(soup.body)
            self.store_csv(data)

        self.concat_csv()

    def store_csv(self, data):
        file_path = self.DATA_PATH + '/' + str(datetime.now()) + '.csv'
        pd.DataFrame(data, columns=self.DATA_FIELDS).to_csv(
            file_path, sep=',', index=False)

    def concat_csv(self):
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
        soup = self.make_soup()
        desc = soup.find(class_='desc').span.text
        n_start = desc[0]
        n_res = desc[2:4]
        n_end = [int(s) for s in desc.split() if s.isdigit()][0]
        return (int(n_start), int(n_res), int(n_end))

    def make_soup(self, start: int = 0):
        url = self.URL + '&start=' + str(start)
        print('Fetching : ', url)
        html = self.fetch(url)
        return BeautifulSoup(html, "html.parser")

    def extract_info(self, soup):
        tmp_data = []
        films = soup.find_all(class_='lister-item mode-advanced')
        for film in films:
            img_div, title, * \
                rest = film.find_all('a', {'href': re.compile(r'^/title/')})

            tmp_data.append([
                title.get('href').split('/')[2],  # IMBd_id
                title.text,
                self.sanitize_text(
                    film.find('span', {'class': 'lister-item-year'}).text),
                self.sanitize_text(
                    film.find('span', {'class': 'runtime'}).text),
                self.sanitize_text(film.find('span', {'class': 'genre'}).text),
                datetime.now()
            ])

        return tmp_data

    def sanitize_text(self, txt: str) -> str:
        char_list = [' ', '\(', '\)', '\n']
        return re.sub('|'.join(char_list), '', txt)

    def construct_url(self) -> str:
        url = self.BASE_URL
        for i, x in enumerate(self.URL_PARAMS.items()):
            if (i == 0 and self.BASE_URL[-1] != '?'):
                url += '?'

            url += x[0] + '=' + str(x[1]) + '&'
            if (i == len(self.URL_PARAMS) - 1):
                url = url[:-1]

        return url

    def fetch(self, url: str = None) -> str or bool:
        if url is None:
            url = self.URL

        req = requests.get(url)
        status = req.status_code
        if str(status).startswith('2'):
            return req.text
        else:
            return False

        ###############################


URL_PARAMS = {
    'groups': 'top_250',
    'sort': 'user_rating,desc'
}

scrapper = Scrapper(url_params=URL_PARAMS)
scrapper.crawl()

'''
QUESTIONS :
- une methode par donnée ?
- unicité de l'information ? -> intrinsequement lié au web scrapping
- données non structurée ?
- obj
- monitoring
- html >



- diff studios
- diff en fn des années
- score en fn de la durée
- corrs
'''
