import MySQLdb
import datetime
import os
import sys
import csv
import logging
import logging.handlers
from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import time
import traceback
from datetime import datetime

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
        driver.quit()
    except Exception as exe:
        process_logger.debug(str(traceback.format_exc()))
        process_logger.debug("Exception while closing driver.")
        pass

def start_process(crawl_type):
    global connection
    global cursor
    connection, cursor = open_mysql_connection()
    display, driver = open_driver()


    start_url = 'https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware'
    page_extract(start_url,crawl_type)

def page_extract(start_url,crawl_type):
    connection, cursor = open_mysql_connection()
    display, driver = open_driver()
    driver.get(start_url)
    time.sleep(5)
    #driver_data(driver, start_url, crawl_type)
    #if (crawl_type != 'keepup') and (crawl_type == 'catchup'):
    page_url = 'https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/page/%s'
    max_pages_numbers =  driver.find_elements_by_xpath('//li[@class="pagesnumber"]//a')
    max_pages_number =  max_pages_numbers[-1].get_attribute('href')
    print max_pages_number

    if max_pages_number:
       	 max_pages_number =  int(max_pages_number.split('/')[-1]) + 1
	 for i in range(int(max_pages_number)):
	     if i >= 1 :
                 next_page_url = 'https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/page/%s' % i
                 #print next_page_url
                 #page_next_extract(next_page_url,start_url)

                 if i ==1 and crawl_type == 'keepup':
                    page_next_extract(next_page_url,start_url,crawl_type)
                 elif i >=1  and crawl_type == 'catchup':

                    page_next_extract(next_page_url,start_url,crawl_type)


    close_driver(display, driver)
    close_mysql_connection(connection, cursor)


def page_next_extract(next_page_url,start_url,crawl_type):
    print next_page_url
    connection, cursor = open_mysql_connection()


    display, driver = open_driver()
    driver.get(next_page_url)
    malware_links = driver.find_elements_by_xpath('//div[@class="ContainerListTitle1"]//a')
    for data_links in malware_links:
	url = data_links.get_attribute('href')
        print url
	insert_query = 'insert into trend_123(data_links,reference_url,crawl_type,created_at,modified_at,crawl_status) values(%s,%s,%s,now(),now(),0)on duplicate key update modified_at = now()'
        #import pdb;pdb.set_trace()
        listing_data = (url,start_url,crawl_type)
        cursor.execute(insert_query, listing_data)
        connection.commit()

    close_driver(display, driver)
    close_mysql_connection(connection, cursor)


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
    connection = MySQLdb.connect(host = 'localhost', user = 'root', passwd = 'hdrn59!',db = 'selinum')
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

    parser = OptionParser()
    parser.add_option('-a', '--crawl_type', default='', help='Enter keepup/Enter catchup')

    (options, args) = parser.parse_args()
    crawl_type = options.crawl_type.strip()
    result = start_process(crawl_type)
