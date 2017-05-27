from collections import namedtuple
from lxml import etree
from lxml import html
import json
import mysql.connector
import os
import requests
import schedule
import time


class Loader:
    HEADERS = {'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36')}

    def __init__(self, conn):
        self._conn = conn

    def Load(self):
        raise NotImplementedError("subclasses must override Load()")


class CompanyLoader(Loader):
    PAGE_URL = 'http://edge.pse.com.ph/companyDirectory/search.ax'

    def __init__(self, conn):
        Loader.__init__(self, conn)
        self._current_page = 0
        self._total_pages = 1

    def Load(self):
        while self._HasMorePages():
            self._LoadCompaniesFromNextPage()
        self._conn.commit()

    def _LoadCompaniesFromNextPage(self):
        page = self._GetNextPage()
        self._current_page, self._total_pages = self._GetPageNumber(page)
        companies = self._GetCompaniesFromPage(page)
        self._SaveCompanies(companies)

    def _HasMorePages(self):
        return self._current_page < self._total_pages

    def _GetNextPage(self):
        payload = {'sector': 'ALL', 'subsector': 'ALL',
                   'pageNo': self._current_page + 1}
        page = requests.post(self.PAGE_URL, data=payload, headers=self.HEADERS)
        tree = html.fromstring(page.content)
        return tree

    def _GetPageNumber(self, page):
        count_tag = page.xpath('//span[@class="count"]/text()')[0]
        lbrace_index = count_tag.index('[')
        slash_index = count_tag.index('/')
        rbrace_index = count_tag.index(']')
        current_page = int(count_tag[lbrace_index + 1:slash_index].strip())
        page_total = int(count_tag[slash_index + 1:rbrace_index].strip())
        return current_page, page_total

    def _GetCompaniesFromPage(self, page):
        Company = namedtuple(
            'Company', ['name', 'ticker', 'sector', 'subsector'])
        companies = []
        tr = page.xpath('//tbody/tr')
        for tr_i in tr:
            name = tr_i.findall('td[1]/a')[0].text.strip()
            ticker = tr_i.findall('td[2]/a')[0].text.strip()
            sector = tr_i.findall('td[3]')[0].text.strip()
            subsector = tr_i.findall('td[4]')[0].text.strip()
            companies.append(Company(name, ticker, sector, subsector))
        return companies

    def _SaveCompanies(self, companies):
        for company in companies:
            if not self._IsCompanyAlreadySaved(company.ticker):
                self._SaveCompany(company)
                print 'Successfully saved company:', company.ticker
            else:
                print company.ticker, 'is already present. skipping...'

    def _IsCompanyAlreadySaved(self, ticker):
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT 1 FROM Company WHERE ticker = %s", [ticker])
        cursor.fetchall()
        return cursor.rowcount > 0

    def _SaveCompany(self, company):
        insert_stmt = ("INSERT INTO Company (name, ticker, sector, subsector) "
                       "VALUES (%s, %s, %s, %s)")
        self._conn.cursor().execute(insert_stmt, company)


class PriceLoader(Loader):
    JSON_URL = 'https://ph.colfinancial.com/ape/colcharts/jason.asp'

    def Load(self):
        for (ticker,) in self._GetTickers():
            params = {'symbol': ticker}
            response = requests.get(
                self.JSON_URL, params=params, headers=self.HEADERS)
            if response.status_code == requests.codes.ok:
                try:
                    prices = json.loads(response.content)
                except ValueError:
                    print 'Failed to parse the data for', ticker
                    continue
                self._SavePrices(ticker, prices)
            else:
                print 'Failed to load prices for:', ticker
        self._conn.commit()

    def _GetTickers(self):
        cursor = self._conn.cursor()
        cursor.execute("SELECT ticker FROM Company ORDER BY ticker")
        return cursor.fetchall()

    def _SavePrices(self, ticker, json_data):
        max_date = self._GetMaxDateAvailableForTicker(ticker)
        print 'Latest available date for', ticker, 'is', max_date
        for price in json_data:
            date = price['Date']
            if date <= max_date:
                continue
            open = price['Open']
            high = price['High']
            low = price['Low']
            close = price['Close']
            volume = price['Volume']
            price = (ticker, date, open, high, low, close, volume)
            self._SavePrice(price)
            print 'Successfully saved prices for:', ticker, 'at', date

    def _GetMaxDateAvailableForTicker(self, ticker):
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT max(date) FROM Price WHERE ticker = %s", [ticker])
        return cursor.fetchone()[0]

    def _SavePrice(self, price):
        insert_stmt = ("INSERT INTO Price "
                       "(ticker, date, open, high, low, close, volume) "
                       "VALUES (%s, %s, %s, %s, %s, %s, %s)")
        self._conn.cursor().execute(insert_stmt, price)


def RunApp():
    conn = mysql.connector.connect(user=os.getenv('MYSQL_USER', 'root'),
                                   password=os.getenv('MYSQL_PASSWORD', ''),
                                   host=os.getenv('MYSQL_HOST', '127.0.0.1'),
                                   database=os.getenv('MYSQL_DB', 'pseviz'))
    try:
        CompanyLoader(conn).Load()
        PriceLoader(conn).Load()
    finally:
        conn.cursor().close()
        conn.close()

if __name__ == '__main__':
    schedule.every().day.at("16:00").do(RunApp)
    while True:
      schedule.run_pending()
      time.sleep(30)  # seconds
