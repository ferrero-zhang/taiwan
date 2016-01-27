# -*- coding: utf-8 -*-

import re
import time
import pymongo
from BeautifulSoup import BeautifulSoup, SoupStrainer
from selenium import webdriver
from urllib import quote
from time4stamp import time2stamp
import logging

BASE_URL = "http://www.alexa.com/siteinfo/{website}"
USER_NAME = '714445945@qq.com'
USER_PWD = 'zxcv1234'

#MONGOD_HOST = '113.10.156.125'
MONGOD_HOST = 'localhost'
MONGOD_PORT = 27017
MONGOD_DB = 'taiwan'
MONGOD_COLLECTION = 'fb_person'

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
        login_url = 'https://mobile.facebook.com'
        self.client.get(login_url)

        username = self.client.find_element_by_name("email")
        pwd = self.client.find_element_by_name("pass")
        submit = self.client.find_element_by_name("login")

        USER = USER_NAME
        PWD = USER_PWD
        username.send_keys(USER)
        pwd.send_keys(PWD)
        submit.click()
        # time.sleep(5)

        self.db = _default_mongo()

    def spider_website(self, keyword,author):
       
        search = self.client.find_element_by_name("query")
        search.clear()
        search.send_keys(keyword)
        search_submit = self.client.find_element_by_xpath('/html/body/div/div/div[1]/div/form/table/tbody/tr/td[3]/input')
        search_submit.click()
        person = self.client.find_element_by_xpath('/html/body/div/div/div[2]/div[2]/div/div/div[2]/div/div/form/div/div/table/tbody/tr[1]/td[2]/a')
        #person.click()

        total_favourity = self.client.find_element_by_xpath('/html/body/div/div/div[2]/div[2]/div/div/div[2]/div/div/form/div/div/table/tbody/tr/td[2]/div').text
        total_favourity = total_favourity.replace(',','')
        total_favourity = total_favourity.split(' ')[0]
        #print total_favourity
        
        now_date = self.get_now_date()
        _id = author + now_date        
        item = {
            '_id':_id,
            'total_favourity':total_favourity,
            'timestamp':time.time(),
            'person':author,
            'date':now_date
            }
        self.process_item(item)
        #print item
        


    def get_now_date(self):
        return time.strftime('%Y-%m-%d', time.localtime(time.time()))

    def process_item(self,item):
        if self.db[MONGOD_COLLECTION].find({"_id":item["_id"]}).count():
            self.update_item(item)
        else:
            try:
                item["first_in"] = time.time()
                item["last_modify"] = item["first_in"]
                self.db[MONGOD_COLLECTION].insert(item)
            except:
                self.update_item(item)


    def update_item(self,item):
        item['last_modify'] = time.time()
        updates_modifier = {'$set':item}
        self.db[MONGOD_COLLECTION].update({'_id':item['_id']},updates_modifier)


def main():
    logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='facebook.log',
                filemode='a')
    s = Spider()
    keywords = [u'蔡英文',u'朱立倫',u'宋楚瑜']
    i = 0
    for keyword in keywords:
        i = i + 1
        #print i
        try:
            logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='facebook.log',
                filemode='a')
            logging.info(' start spider '+keyword +' at: ' + str(ts2HMS_1(time.time())) )
            print 'start spider %s' % keyword
            if(i==1):
                s.spider_website(keyword,'tsaiingwen')
            elif(i==2):
                s.spider_website(keyword,'llchu')
            elif(i==3):
                s.spider_website(keyword,'soong')
            logging.info(' end spider '+keyword +' at: ' + str(ts2HMS_1(time.time())) )
            print 'end spider %s' % keyword
            s.client.close()
        except Exception,e:
            logging.info(e)        
            s.client.close()


if __name__ == '__main__':
    main()
