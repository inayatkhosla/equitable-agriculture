"""
scrapers.py:
    Scrapes commodity price and arrival data at wholesale agricultural markets 
    across India from 'http://agmarknet.gov.in/'. Configured to optionally 
    handle serverless deployment

    PRICES
    MandiPriceScraper (cls): Scrapes prices over a date range and writes output

    ARRIVALS
    MandiArrivalScraper (cls): Scrapes arrivals for a single date
    MandiQuantityScraper (cls): Wrapper - scrapes, processes, and writes output
"""


import pathlib
import os
import time
import json

from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import re
import math
import pandas as pd
from pandas.io.json import json_normalize

from sqlalchemy import *
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import src.helpers as h


class MandiPriceScraper(object):
    """
    Scrapes prices over a date range and writes output

    Args:
        commodity (str): Commodity to scrape data for
        state (str): State to scrape data for
        start (str): Start of period to scrape data for
        end (str): End of period to scrape data for
        serverless (bool): Lambda execution flag
        writetodb (bool): Flag for inserting into db or saving json
    """
    def __init__(self, commodity, state, start=None, end=None, serverless=True, writetodb=True):
        self.commodity = commodity
        self.state = state
        self.start = start
        self.end = end
        self.serverless = serverless
        self.writetodb = writetodb
        self.URL = 'http://agmarknet.gov.in/'
        self.DRIVER_DIR = '/Users/inayatkhosla/Downloads/chromedriver'
        self.ROOTDIR = 'data/'
        self.DBTABLE = 'prices_test' #Change this to 'prices' before zipping
        if not self.start:
            self.start = str(pd.to_datetime('today').date() - pd.to_timedelta(1, unit='d'))
            self.end = str(pd.to_datetime('today').date() - pd.to_timedelta(1, unit='d'))
        
        
    def setup_driver_reg(self):
        chrome_options = Options()  
        chrome_options.add_argument("--headless")
        self.driver = webdriver.Chrome(executable_path=self.DRIVER_DIR, options=chrome_options)
        
    
    def setup_driver_lambda(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1280x1696')
        chrome_options.add_argument('--user-data-dir=/tmp/user-data')
        chrome_options.add_argument('--hide-scrollbars')
        chrome_options.add_argument('--enable-logging')
        chrome_options.add_argument('--log-level=0')
        chrome_options.add_argument('--v=99')
        chrome_options.add_argument('--single-process')
        chrome_options.add_argument('--data-path=/tmp/data-path')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--homedir=/tmp')
        chrome_options.add_argument('--disk-cache-dir=/tmp/cache-dir')
        chrome_options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36')
        chrome_options.binary_location = os.getcwd() + "/bin/headless-chromium"
        self.driver = webdriver.Chrome(chrome_options=chrome_options)
    
    
    def setup_driver(self):
        if self.serverless:
            self.setup_driver_lambda()
        else:
            self.setup_driver_reg()
    
    
    def open_page(self):
        self.driver.get(self.URL)
        
        
    def select_scrape_type(self):
        element = Select(self.driver.find_element_by_id('ddlArrivalPrice'))
        element.select_by_visible_text('Price')
        
        
    def select_commodity(self):
        element = Select(self.driver.find_element_by_id('ddlCommodity'))
        element.select_by_visible_text(self.commodity)
        #self.commodities = [o.text for o in element.options]
        
        
    def select_state(self):
        element = Select(self.driver.find_element_by_id('ddlState'))
        element.select_by_visible_text(self.state)
        
        
    def select_daterange(self):
        startdate = self.driver.find_element_by_id('txtDate')
        startdate.clear()
        startdate.send_keys(self.start)
        endate = self.driver.find_element_by_id('txtDateTo')
        endate.clear()
        endate.send_keys(self.end)
        time.sleep(3)
        endate.send_keys(Keys.ENTER)
    
    
    def populate_dropdowns(self):
        self.select_scrape_type()
        self.select_commodity()
        self.select_state()
        time.sleep(3)
        self.select_daterange()
        time.sleep(3)
        
        
    def get_pagecount(self):
        heading = self.driver.find_element_by_id('cphBody_LabComName').text
        if 'Total' in heading:
            self.data = 'Yes'
            record_count = int(re.findall(r'\d+\d*', heading.split(' ')[-1])[0])
            self.page_count = int(math.ceil(record_count/50))
            print('Page Count: {}'.format(self.page_count))
        else:
            self.data = 'No'
            print('No Available Data')

        
        
    def extract_prices(self):
        table_rows = self.driver.find_elements_by_xpath('//table[@class="tableagmark_new"]//tr')
        for row in table_rows[1:]:
            td = row.find_elements_by_xpath(".//td/span")
            if len(td) > 0:
                record = {
                    'commodity': td[2].text,
                    'date': pd.to_datetime(td[8].text),
                    'state': self.state,
                    'district': td[0].text,
                    'market': td[1].text,
                    'grade': td[4].text,
                    'variety': td[3].text,
                    'max_price': pd.to_numeric(td[6].text).astype(float),
                    'min_price': pd.to_numeric(td[5].text).astype(float),
                    'modal_price': pd.to_numeric(td[7].text).astype(float)
                    } 
            self.prices.append(record)
    
    
    def scrape_prices(self):
        counter = 1
        self.prices = []
        while counter <= self.page_count:
            print('Scraping {} of {}'.format(counter, self.page_count))
            self.extract_prices()
            try:
                next_icon = self.driver.find_element_by_xpath('//input[contains(@src,"Next.png")]')
                next_icon.send_keys(Keys.SPACE)
                counter +=1
                time.sleep(5)
            except NoSuchElementException:
                break
        self.prices = [i for n, i in enumerate(self.prices) if i not in self.prices[n + 1:]]

                                
    def write_locally(self):
        path = pathlib.Path(self.ROOTDIR)
        path.mkdir(parents=True, exist_ok=True)
        self.path = path
        fn = 'prices_{}_{}_{}.json'.format(self.state, self.start, self.end)
        with open((self.path/fn), 'w') as outfile:
            json.dump(self.prices, outfile)
        
        
    def write_db(self):
        engine = h.db_connect()
        Session = sessionmaker(bind=engine)
        session = Session()
        Base = declarative_base(engine)
        metadata = MetaData(bind=engine)
        class Prices(Base):
            __table__ = Table(self.DBTABLE, metadata, autoload=True)
        for price in self.prices:
            row = Prices(**price)
            try:
                session.add(row)
            except IntegrityError:
                print('Integrity Error: Duplicate Record')
                continue
        session.commit()
        session.close()
        
        
    def write(self):
        if self.writetodb:
            self.write_db()
        else:
            self.write_locally()
        print('Written')

        
    def run(self):
        self.setup_driver()
        self.open_page()
        self.populate_dropdowns()
        self.get_pagecount()
        if self.data == 'Yes':
            self.scrape_prices()
            self.write()
        self.driver.close()



class MandiArrivalScraper(object):
    """
    Scrapes arrivals data

    Args:
        commodity (str): Commodity to scrape data for
        state (str): State to scrape data for
        start (str): Start of period to scrape data for
        end (str): End of period to scrape data for
        serverless (bool): Lambda execution flag
    """
    def __init__(self, commodity, state, start, end, serverless):
        self.commodity = commodity
        self.state = state
        self.start = start
        self.end = end
        self.serverless = serverless
        self.URL = 'http://agmarknet.gov.in/'
        self.DRIVER_DIR = '/Users/inayatkhosla/Downloads/chromedriver'

            
    def setup_driver_reg(self):
        chrome_options = Options()  
        chrome_options.add_argument("--headless")
        self.driver = webdriver.Chrome(executable_path=self.DRIVER_DIR, options=chrome_options)
        
    
    def setup_driver_lambda(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1280x1696')
        chrome_options.add_argument('--user-data-dir=/tmp/user-data')
        chrome_options.add_argument('--hide-scrollbars')
        chrome_options.add_argument('--enable-logging')
        chrome_options.add_argument('--log-level=0')
        chrome_options.add_argument('--v=99')
        chrome_options.add_argument('--single-process')
        chrome_options.add_argument('--data-path=/tmp/data-path')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--homedir=/tmp')
        chrome_options.add_argument('--disk-cache-dir=/tmp/cache-dir')
        chrome_options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36')
        chrome_options.binary_location = os.getcwd() + "/bin/headless-chromium"
        self.driver = webdriver.Chrome(chrome_options=chrome_options)
    
    
    def setup_driver(self):
        if self.serverless:
            self.setup_driver_lambda()
        else:
            self.setup_driver_reg()
    
    
    def open_page(self):
        self.driver.get(self.URL)
        
        
    def select_scrape_type(self):
        element = Select(self.driver.find_element_by_id('ddlArrivalPrice'))
        element.select_by_visible_text('Arrival')
        
        
    def select_commodity(self):
        element = Select(self.driver.find_element_by_id('ddlCommodity'))
        element.select_by_visible_text(self.commodity)
        #self.commodities = [o.text for o in element.options]
        
        
    def select_state(self):
        element = Select(self.driver.find_element_by_id('ddlState'))
        element.select_by_visible_text(self.state)
        
        
    def select_daterange(self):
        startdate = self.driver.find_element_by_id('txtDate')
        startdate.clear()
        startdate.send_keys(self.start)
        endate = self.driver.find_element_by_id('txtDateTo')
        endate.clear()
        endate.send_keys(self.end)
        time.sleep(3)
        endate.send_keys(Keys.ENTER)
    
    
    def populate_dropdowns(self):
        self.select_scrape_type()
        self.select_commodity()
        self.select_state()
        time.sleep(3)
        self.select_daterange()
        time.sleep(3)
        
        
    def unfurl_quantities(self):
        while True:
            try:
                plus_icon = self.driver.find_element_by_xpath('//input[contains(@src,"plus.png")]')
                plus_icon.send_keys(Keys.SPACE)
                time.sleep(1)
            except NoSuchElementException:
                break

                
                
    def extract_quantities(self):
        m = self.driver.find_elements_by_xpath('//span[contains(@id,"MarketName")]')
        q = self.driver.find_elements_by_xpath('//span[contains(@id,"Lab2Arrival")]')
        quantities = list(zip([i.text for i in m], [i.text for i in q]))
        self.arrivals = {
            'commodity': self.commodity,
            'date': self.start,
            'state': self.state,
            'Arrivals': quantities
                    }
                

    def run(self):
        self.setup_driver()
        self.open_page()
        self.populate_dropdowns()
        self.unfurl_quantities()
        self.extract_quantities()
        self.driver.close()


class MandiQuantityScraper(object):
    """
    Wrapper - scrapes, processes, and writes arrival data

    Args:
        commodity (str): Commodity to scrape data for
        state (str): State to scrape data for
        start (str): Start of period to scrape data for
        end (str): End of period to scrape data for
        serverless (bool): Lambda execution flag
        writetodb (bool): Flag for inserting into db or saving json
    """
    def __init__(self, commodity, state, start=None, end=None, serverless=True, writetodb=True):
        self.commodity = commodity
        self.state = state
        self.start = start
        self.end = end
        self.serverless = serverless
        self.writetodb = writetodb
        self.ROOTDIR = 'data/'
        self.DBTABLE = 'arrivals_test' #Change this to 'arrivals' before zipping
        if not self.start:
            self.start = str(pd.to_datetime('today').date() - pd.to_timedelta(1, unit='d'))
            self.end = str(pd.to_datetime('today').date() - pd.to_timedelta(1, unit='d'))
    
    
    def create_engine(self):
        self.engine = h.db_connect()
    
    
    def get_locationmaps(self):
        conn = self.engine.connect()
        self.lm = pd.read_sql('select * from location_map', con=conn)
        conn.close()
        
    
    def get_timeperiods(self):
        dr = pd.date_range(self.start, self.end, freq='D')
        self.times = [t.strftime('%d-%b-%Y') for t in dr]

        
    def scrape(self):
        daily_arrivals = []
        for i in self.times:
            print('Pulling {}'.format(i))
            mas = MandiArrivalScraper(self.commodity, self.state, i, i, self.serverless)
            mas.run()
            daily_arrivals.append(mas.arrivals)
            time.sleep(3)
        self.daily_arrivals = daily_arrivals
        
    
    def process(self):
        arrivals = json_normalize(self.daily_arrivals, 'Arrivals', ['date', 'state', 'commodity'])
        arrivals.rename(columns={0: 'market', 1: 'quantity'}, inplace=True)
        arrivals['date'] = pd.to_datetime(arrivals['date'])
        arrivals['quantity'] = pd.to_numeric(arrivals['quantity']).astype(float)
        arrivals.reset_index(drop=True, inplace=True)
        dmaps = self.lm[['district', 'market']].drop_duplicates().set_index('market')['district'].to_dict()
        arrivals['district'] = arrivals['market'].map(dmaps)
        arrivals = arrivals.drop_duplicates()
        arrivals = arrivals[['commodity','date','state','district','market','quantity']]
        self.arrivals = arrivals.to_dict('records')
        
    
    def write_locally(self):
        path = pathlib.Path(self.ROOTDIR)
        path.mkdir(parents=True, exist_ok=True)
        self.path = path
        fn = 'prices_{}_{}_{}.json'.format(self.state, self.start, self.end)
        with open((self.path/fn), 'w') as outfile:
            json.dump(self.prices, outfile)
        
        
    def write_db(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        Base = declarative_base(self.engine)
        metadata = MetaData(bind=self.engine)
        class Arrivals(Base):
            __table__ = Table(self.DBTABLE, metadata, autoload=True)
        for arrival in self.arrivals:
            row = Arrivals(**arrival)
            try:
                session.add(row)
            except IntegrityError:
                print('Integrity Error: Duplicate Record')
                continue
        session.commit()
        session.close()
        
        
    def write(self):
        if self.writetodb:
            self.write_db()
        else:
            self.write_locally()
        print('Written')


    def run(self):
        self.create_engine()
        self.get_locationmaps()
        self.get_timeperiods()
        self.scrape()
        self.process()
        self.write()