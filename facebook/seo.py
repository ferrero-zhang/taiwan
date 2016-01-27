# -*- coding: utf-8 -*-

import re
import time
import pymongo
from BeautifulSoup import BeautifulSoup, SoupStrainer
from selenium import webdriver
from urllib import quote
import logging 
BASE_URL = "http://alexa.chinaz.com/alexa_more.aspx?domain={website}"


#MONGOD_HOST = '113.10.156.125'
MONGOD_HOST = 'localhost'
MONGOD_PORT = 27017
MONGOD_DB = 'taiwan'
MONGOD_COLLECTION = 'alexa'

def ts2HMS_1(ts):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(ts))

def _default_mongo(host=MONGOD_HOST, port=MONGOD_PORT, usedb=MONGOD_DB):
    # 强制写journal，并强制safe
    connection = pymongo.MongoClient(host=host, port=port, j=False, w=1)
    # db = connection.admin
    # db.authenticate('root', 'root')
    db = getattr(connection, usedb)
    return db

class Spider(object):
    def __init__(self):
        self.client = webdriver.Firefox()
        self.db = _default_mongo()

    def spider_website(self, website):
        now_date = self.get_now_date()
        website_url = website
        _id = website + now_date

        data_url = BASE_URL.format(website=quote(website))           
        self.client.get(data_url)
        #home_page_soup = BeautifulSoup(self.client.page_source)
        '''
        ip = home_page_soup.find("span", {"id": "ipavg"}).text
        ip = ip.replace(',','')
        vp = home_page_soup.find("span", {"id": "pvavg"}).text
        vp = vp.replace(',','')
        '''
        
        ip = self.client.find_element_by_xpath("/html/body/div[2]/div[13]/div[2]/table/tbody/tr[2]/td").text
        ip = ip.replace(',','').replace(u'≈','')
        
       
        vp = self.client.find_element_by_xpath("/html/body/div[2]/div[13]/div[2]/table/tbody/tr[2]/td[2]").text
        vp = vp.replace(',','').replace(u'≈','')
        
        item = {
            'vp':vp,
            'ip':ip,
            '_id':_id,
        }
        #print item
        self.process_item(item)
    
    def get_now_date(self):
        return time.strftime('%Y-%m-%d', time.localtime(time.time()))

    def process_item(self, item):
        if self.db[MONGOD_COLLECTION].find({"_id": item["_id"]}).count():
            self.update_item(item)
        else:
            try:
                item['first_in'] = time.time()
                item['last_modify'] = item['first_in']
                self.db[MONGOD_COLLECTION].insert(item)
            except pymongo.errors.DuplicateKeyError:
                self.update_item(item)

    def update_item(self, item):
        item['last_modify'] = time.time()
        updates_modifier = {'$set': item}
        self.db[MONGOD_COLLECTION].update({'_id': item['_id']}, updates_modifier)


def main():
    logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='facebook.log',
                filemode='a')
    s = Spider()
    URLS = [
        "http://iing.tw/",
        "http://www.pfp.org.tw/",
        "http://www.doer.tw/",
        "http://www.soong.tw/",
        "http://onetaiwan.tw/"
        ]
    for url in URLS:
        try:
            logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='facebook.log',
                filemode='a')
            logging.info(' start spider '+ url +'at: ' + str(ts2HMS_1(time.time())) )
            print 'start spider %s' % url
            s.spider_website(url)
            logging.info(' end spider '+ url +'at: ' + str(ts2HMS_1(time.time())) )
            print 'end spider %s' % url
            s.client.close()
        except Exception,e:
            logging.info(e)        
            s.client.close()


if __name__ == '__main__':
    main()
