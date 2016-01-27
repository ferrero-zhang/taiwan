# -*- coding: utf-8 -*-

import re
import time
import pymongo
from BeautifulSoup import BeautifulSoup, SoupStrainer
from selenium import webdriver
from urllib import quote
import MySQLdb
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
        login_url = 'http://udn.com/news/index'
        self.client.get(login_url)
        self.db = _default_mongo()

    def spider_website(self,keyword):

        search = self.client.find_element_by_id("search_kw")
        search.send_keys(keyword)
        search_submit = self.client.find_element_by_id("search_submit")
        search_submit.click()

        count =0
        next_page = 1
        while next_page:
            soup = BeautifulSoup(self.client.page_source)
            results = soup.find('div',{'id':'search_content'})
            try:
                li_result = results.findAll('dt')
            except:
                next_page = 0
                break
            #print len(li_result)
            #count = 0
            for li in li_result:
            
            
                title = li.find('h3').text
            
                url = li.find('a').get('href')
            
                summary = li.find('p').text
            
                date = li.find('span',{'class':'cat'}).text
                timestamp = date2stamp(date)
            
                source ='und'
                
                keyword = keyword

                item = {
                    'title':title,
                    'summary':summary,
                    'date':date,
                    'keyword':keyword,
                    
                    'url':url,
                    
                    'source':source,
                    'timestamp':timestamp
                }
                #print item
                #print timestamp
                self.process_item(item)
                time.sleep(5)
            try:
                next_page = self.client.find_element_by_xpath('//*[@id="result_list"]/div[3]/gonext/a[1]')
                next_page.click()
            except:
                break
            count += 1
            if(count>10):
                next_page = 0

    def get_now_date(self):
        return time.strftime('%Y-%m-%d', time.localtime(time.time()))

    def process_item(self, item):
        if self.db[MONGOD_COLLECTION].find({"url": item["url"]}).count():
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
        self.db[MONGOD_COLLECTION].update({'url': item['url']}, updates_modifier)


def main():
    logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='news.log',
                filemode='w')
     
    keywords = [u'朱立倫',u'蔡英文',u'宋楚瑜']
    for keyword in keywords:
        s = Spider()
        logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='news.log',
                filemode='w')
        logging.info('und start spider'+ keyword  )
        try:
            s.spider_website(keyword)
            logging.info('und end spider'+ keyword  )
            s.client.close()
        except Exception,e:
            logging.info(e)        
            s.client.close()
        
def date2stamp(date):
    dd = date.split(u'：')
    dd = dd[1]
    timestamp =int(time.mktime(time.strptime(dd,'%Y/%m/%d')))
    return timestamp

if __name__ == '__main__':
    main()
