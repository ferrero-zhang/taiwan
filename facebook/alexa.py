# -*- coding: utf-8 -*-

import re
import time
import pymongo
from BeautifulSoup import BeautifulSoup, SoupStrainer
from selenium import webdriver
from urllib import quote
import logging

BASE_URL = "http://www.alexa.com/siteinfo/{website}"
USER_NAME = '714445945@qq.com'
USER_PWD = '22880126'

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
        login_url = 'https://www.alexa.com/secure/login?resource='
        self.client.get(login_url)

        username = self.client.find_element_by_id("email")
        pwd = self.client.find_element_by_id("pwd")
        submit = self.client.find_element_by_id("submit")

        USER = USER_NAME
        PWD = USER_PWD
        username.send_keys(USER)
        pwd.send_keys(PWD)
        submit.click()
        # time.sleep(5)

        self.db = _default_mongo()

    def spider_website(self, website):
        now_date = self.get_now_date()
        website_url = website
        _id = website + now_date

        data_url = BASE_URL.format(website=quote(website))           
        self.client.get(data_url)
        home_page_soup = BeautifulSoup(self.client.page_source)
        
        global_rank_span = home_page_soup.find("span", {"class": "globleRank"})
        global_rank = None
        if global_rank_span:
            global_rank = global_rank_span.find("strong", {"class": "metrics-data align-vmiddle"})
            global_rank = global_rank.text
            global_rank = int(re.search(r"awis([0-9]*)", global_rank.replace(',', '')).group(1))

        country_rank_span = home_page_soup.find("span", {"class": "countryRank"})
        country_rank = None
        if country_rank_span:
            country_rank = country_rank_span.find("strong", {"class": "metrics-data align-vmiddle"})
            country_rank = country_rank.text
            country_rank = int(country_rank.replace(',', ''))

        country_geo_ratio = dict()
        demographics_table = home_page_soup.find("table", {"id": "demographics_div_country_table"})
        for tr_ in demographics_table.find("tbody").findAll("tr"):
            try:
                country_site = tr_.find("td").text
                ratio = tr_.find("span", {"class": ""}).text
                country_geo_ratio[country_site] = ratio
            except:
                pass

        engagement_content_section = home_page_soup.find("section", {"id": "engagement-content"})
        strongs_ = engagement_content_section.findAll("strong", {"class": "metrics-data align-vmiddle"})
        bounce_rate = strongs_[0].text
        daily_page_view = strongs_[1].text
        daily_time = strongs_[2].text

        upstream_content_section = home_page_soup.find("section", {"id": "upstream-content"})
        upstream_ratio = dict()
        for tr in upstream_content_section.find("table").find("tbody").findAll("tr"):
            try:
                upstream_site = tr.find("a").text.replace('.', '/')
            except:
                continue
            ratio = tr.find("td", {"class": "text-right"}).find("span").text
            upstream_ratio[upstream_site] = ratio

        item = {
            "_id": _id,
            "now_date": now_date,
            "website_url": website_url,
            "global_rank": global_rank,
            "country_rank": country_rank,
            "country_geo_ratio": country_geo_ratio,
            "bounce_rate": bounce_rate,
            "daily_page_view": daily_page_view,
            "daily_time": daily_time,
            "upstream_ratio": upstream_ratio
        }

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
        "http://donate.iing.tw/",
        "http://www.pfp.org.tw/123.html",
        "http://www.doer.tw/",
        "http://onetaiwan.tw/",
        "http://www.soong.tw/"
        ]
    for url in URLS:
        print 'start spider %s' % url 
        try:
            logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='facebook.log',
                filemode='a')
            logging.info(' start spider alexa:' + str(ts2HMS_1(time.time())) )
            s.spider_website(url)
            logging.info(' end spider alexa:' + str(ts2HMS_1(time.time())) )
            print 'end spider %s' % url
            s.client.close()
        except Exception,e:
            logging.info(e)        
            s.client.close()


if __name__ == '__main__':
    main()
