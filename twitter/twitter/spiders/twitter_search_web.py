#-*-coding:utf-8-*-

import os
from scrapy.spiders import CrawlSpider
import simplejson as json
from scrapy.http import Request
from twitter.items import TwitterItem
from BeautifulSoup import BeautifulSoup
import re
from urllib import quote
import MySQLdb

BASE_URL = "https://twitter.com/search?f=tweets&vertical=default&q={keywords}&src=typd"


class TweeterSearch(CrawlSpider):
    """usage: scrapy crawl twitter_search_web -a keywords_file=keywords_twitter.txt --loglevel=INFO
    """
    
    name = "twitter_search_web"
    
    '''
    start_urls = [
        "https://twitter.com/search?vertical=default&q=%E8%94%A1%E8%8B%B1%E6%96%87&src=typd"
    ]
    
    start_urls = [
        "https://twitter.com/search?f=tweets&vertical=default&q=%E8%94%A1%E8%8B%B1%E6%96%87&src=typd"
    ]
    
    def __init__(self,keywords_file):
        self.keywords = []
        f = open(os.path.join("./twitter/source/", keywords_file))
        for line in f:
            if '!' in line:
                strip_no_querys = []
                querys = line.strip().lstrip('(').rstrip(')').split(' | ')
                for q in querys:
                    strip_no_querys.append(q.split(' !')[0])
                strip_no_querys = '(' + ' | '.join(strip_no_querys) + ')'
                line = strip_no_querys
            keywords_para = line.strip()
            self.keywords.append(keywords_para)
        f.close()
    '''
    def __init__(self):
        self.keywords = []
        self.persons = []
        self.langs = []
        conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',db='taiwan',port=3306,charset='utf8')
        cur = conn.cursor()
        ppl = conn.cursor()
        lang = conn.cursor()
        conn.select_db('taiwan')
        count = cur.execute('select keyword,person,lang from keywords')
        allkeywords = cur.fetchall()
        for li in allkeywords:
            #print li[0]
            self.keywords.append(li[0].encode('utf-8'))
            ppl.execute('select person from person where id='+str(li[1]))
            lang.execute('select  lang from lang where id='+str(li[2]))
            people = ppl.fetchall()
            lan = lang.fetchall()
            self.persons.append(people[0][0])
            self.langs.append(lan[0][0])
    def start_requests(self):
        count = 0
        for keyword in self.keywords:
            #print type(keyword)
            search_url = self.get_search_url(keyword)
            request = Request(search_url)
            request.meta['keyword'] = keyword
            request.meta['candidate'] = self.persons[count]
            request.meta['lang'] = self.langs[count]
            
            #print self.persons[count],self.langs[count],search_url
            count += 1
            yield request
        
    def parse(self, response):
        keyword = response.meta['keyword']
        candidate = response.meta['candidate']
        lang = response.meta['lang']
        resp = response.body
        soup = BeautifulSoup(resp)
        content = soup.find('ol',{'id':'stream-items-id'})
        li_result = content.findAll('li',{'class':'js-stream-item stream-item stream-item expanding-stream-item\n'})
        items = []
        for li in li_result:
            id_tweet = li.get('data-item-id')
            try:
                text = li.find('p',{'class':'TweetTextSize  js-tweet-text tweet-text'}).text
            except:
                continue
            try:
                screen_name = li.find('strong',{'class':'fullname js-action-profile-name show-popup-with-id'}).text
            except:
                continue
            try:
                user = li.find('span',{'class':'username js-action-profile-name'}).text
            except:
                continue
            try:
                timestamp = li.find('span',{'data-long-form':'true'}).get('data-time-ms')
                created_at = int(timestamp) /1000
            except:
                continue
            try:
                retweet_count = li.find('span',{'class':'ProfileTweet-actionCountForPresentation'}).text
            except:
                continue
            try:
                favourites_count = li.find('span',{'class':'ProfileTweet-actionCountForPresentation'}).text
            except:
                continue
            news_item = {'id_tweet':id_tweet,'text':text,'screen_name':screen_name,'user':user,'created_at':created_at,\
                        'retweet_count':retweet_count,'favourites_count':favourites_count,'lang':lang,'candidate':candidate}
            item = TwitterItem()
            for key in TwitterItem.RESP_TWEET_WEB:
                item[key] = news_item[key]
            item['keyword'] = keyword.decode('utf-8')
            items.append(item)
            #print item
        return items
        
    def get_search_url(self, keyword):
        return BASE_URL.format(keywords=keyword)        
