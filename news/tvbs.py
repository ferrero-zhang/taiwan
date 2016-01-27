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
        login_url = 'http://www.tvbs.com.tw/opencms/search/index.jsp'
        self.client.get(login_url)
        self.db = _default_mongo()

    def spider_website(self, keyword):

        search = self.client.find_element_by_id("topKeyword")
        search.send_keys(keyword)
        search.send_keys(Keys.RETURN)

        next_page = 0
        
        while (next_page<=10):
            soup = BeautifulSoup(self.client.page_source)
            results = soup.find('div',{'id':'list_Gird'})
            li_result = results.findAll('li')
            print len(li_result)
            
            for li in li_result:
                count = 1
                try:
                    title = li.find('h3').text
                except:
                    continue
                try:
                    url = li.find('a').get('href')
                    news_id = url.split('/')[-2].split('-')
                    news_id = news_id[1]
                    url = "http://news.tvbs.com.tw/" + url.split('/')[3] + "/" + url.split('/')[4]
                except:
                    continue
                try:
                    datestr = li.find('img').get('src')
                    datestr = datestr.split('10M')[1]
                    date = datestr.split('.')[0]
                    date = date.replace('_',' ')
                    date = date.replace('-',':')
                    timestamp = int(time.mktime(time.strptime(date,' %Y%m%d %H:%M:%S')))
                    print date,timestamp
                except:
                    date = ''
                    timestamp = ''

                try:
                    summary = li.find('p').text
                except:
                    continue
                source ='tvbs'
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
                count += 1
                #print item
                #print count
                self.process_item(item)
            try:
                next_page += 1
                next = self.client.find_element_by_xpath("/html/body/div[3]/div[2]/div[1]/div/div[3]/div/a["+str(next_page)+"]")
                next.click()
            except:
                next_page = 11
            time.sleep(5)
            
            
                    


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
        logging.info('tvbs start spider'+ keyword  )
        try:
            s.spider_website(keyword)
            logging.info('tvbs end spider'+ keyword  )
            s.client.close()
        except Exception,e:
            logging.info(e)        
            s.client.close()

if __name__ == '__main__':
    main()
