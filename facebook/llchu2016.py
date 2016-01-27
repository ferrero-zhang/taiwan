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
MONGOD_COLLECTION = 'fb_status_llchu'

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

    def spider_website(self, keyword):

        search = self.client.find_element_by_name("query")
        search.send_keys(keyword)
        search_submit = self.client.find_element_by_xpath('/html/body/div/div/div[1]/div/form/table/tbody/tr/td[3]/input')
        search_submit.click()
        person = self.client.find_element_by_xpath('/html/body/div/div/div[2]/div[2]/div/div/div[2]/div/div/form/div/div/table/tbody/tr[1]/td[2]/a')
        person.click()

        history2015 = self.client.find_element_by_xpath('/html/body/div/div/div[2]/div/div/div[2]/div[2]/div/div[3]/a')
        if history2015:
            history2015.click()
        more = self.client.find_element_by_xpath('/html/body/div/div/div[2]/div/div/div/div[2]/a')
        while more:
            counts = self.client.find_elements_by_css_selector("div[id^='u_0_']")
            cts = len(counts) + 1
            print cts
            for count in range(1,cts):
                print count
                try:
                    content = self.client.find_element_by_xpath("/html/body/div/div/div[2]/div/div/div/div/div[2]/div/div[" + str(count)+ "]/div/div[2]/span").text
                except:
                    continue
                c_time = self.client.find_element_by_xpath("/html/body/div/div/div[2]/div/div/div/div/div[2]/div/div[" + str(count)+ "]/div[2]/div").text
                created_time = time2stamp(c_time)
                created_time_timestamp =int(time.mktime(time.strptime(created_time,'%Y-%m-%d %H:%M:%S')))
                
                per_favourity = self.client.find_element_by_xpath("/html/body/div/div/div[2]/div/div/div/div/div[2]/div/div[" + str(count)+ "]/div[2]/div[2]/span/a").text
                per_favourity = per_favourity.replace(',','')
                per_favourity_count = re.split(' ',per_favourity)[0]
                
                item = {
                    'created_time_timestamp':created_time_timestamp,
                    'content':content,
                    'per_favourity_count':per_favourity_count,
                    'author':'llchu',
                    'lang':''
                }
                
                self.process_item(item)
                
                #print content
            more = self.client.find_element_by_xpath('/html/body/div/div/div[2]/div/div/div/div[2]/a').text
            if(more == u'更多'):
                self.client.find_element_by_xpath('/html/body/div/div/div[2]/div/div/div/div[2]/a').click()
            else:
                break


    def get_now_date(self):
        return time.strftime('%Y-%m-%d', time.localtime(time.time()))

    def process_item(self, item):
        '''去重算法
        '''      
        if self.db[MONGOD_COLLECTION].find({"content": item["content"]}).count():
            self.update_item(item)
        else:
            try:
                item['first_in'] = time.time()
                item['last_modify'] = item['first_in']
                item['daily_count'] = {localtime:item['per_favourity_count']}
                self.db[MONGOD_COLLECTION].insert(item)
                #print 'insert'
            except pymongo.errors.DuplicateKeyError:
                self.update_item(item)

    def update_item(self, item):
        #print 'update'
        localtime =  time.strftime("%Y-%m-%d ",time.localtime(time.time()))
        #print type(localtime),localtime
        item['last_modify'] = time.time()
        findli = self.db[MONGOD_COLLECTION].find({'content':item['content']})
        findli = findli[0]
        #print findli['daily_count']
        
        #findli['daily_count'] = ({localtime:item['per_favourity_count']})
        
        if findli['daily_count']:
            try:
                findli['daily_count'].update({localtime:item['per_favourity_count']})
            except:
                findli['daily_count'] = ({localtime:item['per_favourity_count']})
        else:
            findli['daily_count'] = ({localtime:item['per_favourity_count']})
        
        updates_modifier = {'$set':findli}
        self.db[MONGOD_COLLECTION].update({'content': item['content']}, updates_modifier)


def main():
    logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='facebook.log',
                filemode='a')
    s = Spider()
    keywords = [u'朱立倫']
    i = 0
    for keyword in keywords:
        
        print 'start spider %s' % keyword
        try:
            logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='facebook.log',
                filemode='a')
            logging.info(' start spider 2016Y llchu:' + str(ts2HMS_1(time.time())) )
            s.spider_website(keyword)
            logging.info(' end spider 2016Y llchu:' + str(ts2HMS_1(time.time())) )
            print 'end spider %s' % keyword
    
            s.client.close()
        except Exception,e:
            logging.info(e)        
            s.client.close()


if __name__ == '__main__':
    main()
