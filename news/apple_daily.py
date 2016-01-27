# -*- coding: utf-8 -*-

import re
import time
import pymongo
from BeautifulSoup import BeautifulSoup, SoupStrainer
from selenium import webdriver
from urllib import quote
import MySQLdb
from selenium.webdriver.common.keys import Keys
import logging
from config import (MONGOD_HOST,
                    MONGOD_PORT,
                    MONGOD_DB,
                    MONGOD_COLLECTION)


localtime =  time.strftime("%Y-%m-%d ",time.localtime(time.time()))

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
        login_url = 'http://hk.apple.nextmedia.com/'
        self.client.get(login_url)
        self.db = _default_mongo()

    def spider_website(self, keyword):
        
        
        search = self.client.find_element_by_name("search")
        search.send_keys(keyword)
        search.send_keys(Keys.RETURN)
        
        types = self.client.find_element_by_xpath("/html/body/div[8]/div[1]/div[1]/div[1]/div/div[3]/div/div/div/div/div/div/div[3]/table/tbody/tr/td[2]/div/div[2]/div[1]")
        types.click()  
        by_times = self.client.find_element_by_xpath("/html/body/div[8]/div[1]/div[1]/div[1]/div/div[3]/div/div/div/div/div/div/div[3]/table/tbody/tr/td[2]/div/div[2]/div[2]/div[2]/div").click()
        
        next_page = 1
        
        while (next_page<=10):
            soup = BeautifulSoup(self.client.page_source)
            results = soup.find('div',{'class':'gsc-expansionArea'})
            li_result = results.findAll('div',{'class':'gsc-webResult gsc-result'})
            #print len(li_result)
            for li in li_result:
                try:
                    title = li.find('div',{'class':'gs-title gsc-table-cell-thumbnail gsc-thumbnail-left'}).text
                except:
                    continue
                try:
                    url = li.find('a',{'class':'gs-title'}).get('href')
                except:
                    continue
                #view = self.goin(url)
                #print view
                try:
                    summary = li.find('div',{'class':'gs-bidi-start-align gs-snippet'}).text
                except:
                    continue
                
                try:
                    datestr = li.find('div',{'class':'gs-bidi-start-align gs-visibleUrl gs-visibleUrl-long'}).text
                    
                    news_id = datestr.split('/')[-1]
                    date= datestr.split('/')[-2]
                    timestamp = int(time.mktime(time.strptime(date,'%Y%m%d')))
                    
                except:
                    continue
                
                source ='apple_daily'
                keyword = keyword

                item = {
                    'news_id':news_id,
                    'title':title,
                    'summary':summary,
                    'date':date,
                    'keyword':keyword,
                    'url':url,
                    'source':source,
                    'timestamp':timestamp
                    }
                #print item
                #print count
                self.process_item(item)
            #print next_page
            try:
                next_page += 1
                next = self.client.find_element_by_xpath("/html/body/div[7]/div[1]/div[1]/div[1]/div/div[3]/div/div/div/div/div/div/div[5]/div[2]/div/div/div[3]/div[11]/div/div["+str(next_page)+"]")
                next.click()
            except:
                next_page = 11


    def process_item(self, item):
        if self.db[MONGOD_COLLECTION].find({'news_id': item['news_id']}).count():
            self.update_item(item)
        else:
            try:
                item['first_in'] = time.time()
                item['last_modify'] = item['first_in']
                self.db[MONGOD_COLLECTION].insert(item)
                print 'insert'
            except pymongo.errors.DuplicateKeyError:
                self.update_item(item)

    def update_item(self, item):
        print 'update'
        item['last_modify'] = time.time()
        updates_modifier = {'$set':item}
        self.db[MONGOD_COLLECTION].update({'news_id': item['news_id']}, updates_modifier)


def main():
    logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='news.log',
                filemode='a')
     
    keywords = [u'朱立倫',u'蔡英文',u'宋楚瑜']
    for keyword in keywords:
        s = Spider()
        logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='news.log',
                filemode='a')
        logging.info('apple_daily start spider'+ keyword  )
        try:
            s.spider_website(keyword)
            logging.info('apple_daily end spider'+ keyword  )
            s.client.close()
        except Exception,e:
            logging.info(e)        
            s.client.close()

if __name__ == '__main__':
    main()
