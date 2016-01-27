# -*- coding: utf-8 -*-

import re
import time
import pymongo
from BeautifulSoup import BeautifulSoup, SoupStrainer
from selenium import webdriver
from urllib import quote
from time4stamp import date4stamp
import MySQLdb
import logging
from config import (MONGOD_HOST,
                    MONGOD_PORT,
                    MONGOD_DB,
                    MONGOD_COLLECTION)


localtime =  time.strftime("%Y-%m-%d ",time.localtime(time.time()))

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
        login_url = 'https://tw.news.yahoo.com/'
        self.client.get(login_url)
        self.db = _default_mongo()

    def spider_website(self, keyword):

        search = self.client.find_element_by_id("UHSearchBox")
        search.send_keys(keyword)
        search_submit = self.client.find_element_by_id("UHSearchProperty")
        search_submit.click()

        
        byDate = self.client.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div/div[1]/div/div/div/div/ol[1]/li/div/ul/li[2]/span[2]')
        byDate.click()
        
        next_page = self.client.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div/div[1]/div/ol/li/div/div/a[5]')
        count = 0
        while next_page:
            soup = BeautifulSoup(self.client.page_source)
            results = soup.find('ol',{'class':' reg searchCenterMiddle'})
            li_result = results.findAll('li')
            #print len(li_result)
            count += 1
            for li in li_result:
                #count += 1
                try:
                    title = li.find('h3',{'class':'title'}).text
                except:
                    continue
                try:
                    url = li.find('a',{'class':' fz-m'}).get('href')
                except:
                    continue
                try:
                    summary = li.find('div',{'class':'compText mt-5'}).text
                except:
                    continue
                try:
                    user_name = li.find('span',{'class':' fc-12th'}).text
                except:
                    continue
                try:
                    date = li.find('span',{'class':' fc-2nd ml-5'}).text
                    timestamp = date4stamp(date)
                except:
                    continue
                try:
                    data_id = li.find('div',{'class':'dd algo NewsArticle'}).get('data-3eb')
                    #print data_id
                except:
                    continue
                source ='yahoo'
                #lang = lang
                keyword = keyword
                #candidate = candidate
                item = {
                    'title':title,
                    'summary':summary,
                    'date':date,
                    'keyword':keyword,
                    #'candidate':candidate,
                    'url':url,
                    #'lang':lang,
                    'user_name':user_name,
                    'source':source,
                    'timestamp':timestamp
                }
                #print item
                #print count
                self.process_item(item)
            time.sleep(5)
            #print count
            try:
                next_page = self.client.find_element_by_xpath('/html/body/div[1]/div[3]/div[2]/div/div[1]/div/ol/li/div/div/a[5]')
            except:
                next_page = 0
            if next_page:
                next_page.click()
            if(count>=20):
                next_page = 0
                    
                

    def get_now_date(self):
        return time.strftime('%Y-%m-%d', time.localtime(time.time()))

    def process_item(self, item):
        if self.db[MONGOD_COLLECTION].find({'summary': item['summary']}).count():
            for li in self.db[MONGOD_COLLECTION].find({'summary': item['summary']}):
                if(li['user_name']!=item['user_name']):
                    self.update_item(item)
                else:
                    pass
        else:
            try:
                item['last_modify'] = time.time()
                self.db[MONGOD_COLLECTION].insert(item)
                #print 'insert'
            except pymongo.errors.DuplicateKeyError:
                self.update_item(item)

    def update_item(self, item):
        #print 'update'
        item['last_modify'] = time.time()
        updates_modifier = {'$set':item}
        self.db[MONGOD_COLLECTION].update({'summary': item['summary']}, updates_modifier)


def main():
    #s = Spider()
    logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='news.log',
                filemode='w')
    '''
    conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',db='taiwan',port=3306,charset='utf8')
    cur = conn.cursor()
    ppl = conn.cursor()
    lang = conn.cursor()
    conn.select_db('taiwan')
    cur.execute('select keyword,person,lang from keywords')
    keywords = cur.fetchall()
    for keyword in keywords:
        s = Spider()
        logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='news.log',
                filemode='w')
        logging.info('yahoo start spider'+ keyword[0]  )
        #print 'start spider %s' % keyword[0]
        ppl.execute('select  person from person where id='+str(keyword[1]))
        lang.execute('select  lang from lang where id='+str(keyword[2]))
        people = ppl.fetchall()
        lan = lang.fetchall()
        s.spider_website(keyword[0],people[0][0],lan[0][0])
        logging.info('yahoo end spider'+ keyword[0]  )
        #print 'end spider %s' % keyword[0]
        s.client.close()
    '''
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
        logging.info('yahoo start spider'+ keyword + 'at:' + str(ts2HMS_1(time.time())) )
        try:
            s.spider_website(keyword)
            logging.info('yahoo end spider'+ keyword + 'at:' + str(ts2HMS_1(time.time())) )
            s.client.close()
        except Exception,e:
            logging.info(e)        
            s.client.close()


if __name__ == '__main__':
    main()
