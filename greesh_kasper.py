import MySQLdb

import datetime

import os, sys

import csv

import scrapy

import logging

import logging.handlers

#sys.path.append('/root/madhav/madhav/spiders/scrapely-master')

#from scrapely import Scraper

from pyvirtualdisplay import Display

from selenium import webdriver

from selenium.webdriver.common.by import By

from selenium.webdriver.support.wait import WebDriverWait

from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.common.keys import Keys

import time

import traceback

from datetime import datetime

#from elasticsearch import Elasticsearch



from optparse import OptionParser



loggers = {}



def open_driver():

    display = Display(visible=0, size=(800,600))

    display.start() 

    options = webdriver.ChromeOptions()

    options.add_argument('--no-sandbox')

    options.add_argument("--disable-extensions")

    options.add_argument('--headless')

    driver = webdriver.Chrome(chrome_options=options) 

    return display, driver

    

def close_driver(display, driver):

    try:

        display.stop()

        driver.quit()

    except Exception as exe:

        process_logger.debug(str(traceback.format_exc()))

        process_logger.debug("Exception while closing driver.")

        pass



def start_process():

    #es = Elasticsearch(['10.2.0.90:9342'])

    global connection

    global cursor

    connection, cursor = open_mysql_connection()

    Insert_query = "insert into threats_kaspersky_greesh(data_links,page_numbers,reference_url,crawl_type,created_at,modified_at) values(%s,%s, %s,%s,now(),now()) on duplicate key update modified_at = now()"

    close_mysql_connection(connection, cursor)



    display, driver = open_driver()

    #scraper = Scraper()

    #all_datalinks =  sel.xpath('//tr//td[@class="cell_one column_one"]//a//@href').extract()

    start_domain = ['https://threats.kaspersky.com']

    start_urls = ['https://threats.kaspersky.com/en/threat/']



    #insert_query = "insert into threats_kaspersky(data_links,page_numbers,reference_url,crawl_type,created_at,modified_at) values(%s,%s, %s,%s,now(),now()) on duplicate key update modified_at = now()"

    driver.get('https://threats.kaspersky.com/en/threat/')

    all_data_links=driver.find_element_by_xpath('//li[@class="hierarchy_menu"]//a')

    print all_data_links

    main_link =all_data_links.get_attribute('href')

    h_link = 'https://threats.kaspersky.com/'+main_link



    driver.get(h_link)

    all_pages_links=driver.find_elements_by_xpath('//span[@class="table_informations table_hierarchy table_hierarchy_list wo_bg hierarchy_lvl4"]/a[@class="gtm_threats_malware title-end"]')

    for all_page_link in all_pages_links :

        all_pageslinks=all_page_link.get_attribute('href')

        print all_pageslinks

        #listing_data = (pages_datalinks,self.i,self.start_urls[0],'catchup')#crawl_type

        #self.cursor.execute(self.insert_query, listing_data)



    '''if response.status == 200 and self.i < 50:

    self.i = self.i+1

        self.data['page_no'] = str(self.i)

        print self.datai

    driver.get(self.url)

        #yield FormRequest(self.url,self.parse_nextpages,formdata=self.data)

        self.conn.commit()

'''

def myLogger(name):

    log_path = os.path.abspath('logs/')

    try:

        os.mkdir(log_path)

    except:

        pass



    dom_files_path =  os.path.abspath('dom_files/')

    try:

        os.mkdir(dom_files_path)

    except:

        pass



    global loggers

    path = 'logs/kaspersky_%s_%s.log'



    if loggers.get(name):

        return loggers.get(name)

    else:

        logger = logging.getLogger(name)

        logger.setLevel(logging.DEBUG)

        now = datetime.now()

        handler = logging.FileHandler(path %(name, now.strftime("%Y-%m-%d")))

        formatter = logging.Formatter('%(asctime)s - %(filename)s - %(lineno)d - %(funcName)s - %(levelname)s - %(message)s')

        handler.setFormatter(formatter)

        logger.addHandler(handler)

        loggers.update(dict(name=logger))

        return logger

process_logger = myLogger('process')



def open_mysql_connection():

    connection = MySQLdb.connect(host='localhost', user='root', passwd='hdrn59!')

    connection.set_character_set('utf8')

    cursor = connection.cursor()

    process_logger.debug("MySQL connection established.")

    return connection, cursor

def close_mysql_connection(connection, cursor):

    try:

        cursor.close()

        connection.close()

    except Exception as exe:

        process_logger.debug(str(traceback.format_exc()))

        process_logger.debug("Exception while closing driver.")

        pass



if __name__ =="__main__":

    start_process() 
