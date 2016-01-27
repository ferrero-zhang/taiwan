# -*- coding: utf-8 -*-
from langconv import *
import os
import csv
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import time
import datetime
import pymongo
from pymongo import MongoClient
import logging

MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
conn = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
MONGODB_DB = 'taiwan'
MONGOD_COLLECTION_1 = 'news'
MONGOD_COLLECTION_2 = 'twitter'

def ts2HMS(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

def HMS2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d')))

def ts2HMS_1(ts):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(ts))

def sensitive_words():
    sensitive_words = {}
    with open('./sensitive_words.csv','rb')as f:
        reader = csv.reader(f)
        i = 0
        for w,weight in reader:
            sensitive_words[w]= int(weight)
            i += 1

    return sensitive_words

def read_news(start_cal,end_cal,COLLECTION_NAME,sensitive_words):
    '''
    读入给定时间段内的新闻，计算敏感度
    输入数据：
    start_cal:开始计算日期
    end_cal:结束日期
    '''
    mongodb = conn[MONGODB_DB]
    collection = mongodb[COLLECTION_NAME]
    results = collection.find({"timestamp":{"$gte":start_cal, "$lte":end_cal}})
    #print results.count()
    results_test = collection.find()
    #print results_test.count()
    news_sensitive = {}
    i = 0
    for li in results:
        i+= 1
        sensitive_score = 0
        if float(li["timestamp"]) >= start_cal and float(li["timestamp"])<=end_cal:
            i += 1
            news_fan = li["title"]+li["summary"]
            news = Converter('zh-hans').convert(news_fan.decode('utf-8'))
            for k,v in sensitive_words.iteritems():
                if k in news:
                    sensitive_score += news.count(k)*(4-v)

        news_sensitive[li["_id"]] = [sensitive_score,li["title"],li["summary"],li["keyword"]]
        updates_modifier = {'$set':{"sensitive_score":sensitive_score}}
        collection.update({"_id":li["_id"]},updates_modifier)
        #print li["_id"], sensitive_score
        
        
    #print news_sensitive
    #sorted_result = sorted(news_sensitive.iteritems(),key=lambda(k,v):v,reverse=True)
    '''
    with open('./0102/sensitive_score_news.csv','wb')as f:
        writer = csv.writer(f)
        for k,v in news_sensitive.iteritems():
            row = [k]
            row.extend(v)
            writer.writerow((row))
    #print sensitive_score
    '''
def read_twitter(start_cal,end_cal,COLLECTION_NAME,sensitive_words):
    '''
    读入给定时间段内的新闻，计算敏感度
    输入数据：
    start_cal:开始计算日期
    end_cal:结束日期
    '''
    mongodb = conn[MONGODB_DB]
    collection = mongodb[COLLECTION_NAME]
    results = collection.find({"created_at":{"$gte":start_cal,"$lte":end_cal}})
    #print results.count()
    news_sensitive = {}
    i = 0
    with open('./0102/sensitive_score_twitter.csv','wb')as f:
        writer = csv.writer(f)
        for li in results:
            i+= 1
            sensitive_score = 0
            if float(li["created_at"]) >= start_cal and float(li["created_at"])<=end_cal:
                i += 1
                news_fan = li["text"]
                news = Converter('zh-hans').convert(news_fan.decode('utf-8'))
                for k,v in sensitive_words.iteritems():
                    if k in news:
                        sensitive_score += news.count(k)*(4-v)

            news_sensitive[li["_id"]] = [sensitive_score,li["text"].strip('\n'),li["keyword"],ts2HMS(float(li["created_at"])),li["screen_name"],li["user"]]
            updates_modifier = {'$set':{"sensitive_score":sensitive_score}}
            collection.update({"_id":li["_id"]},updates_modifier)
            row = [li["_id"]]
            row.extend(news_sensitive[li["_id"]])
            writer.writerow((row))
        
        
    #print news_sensitive
    #sorted_result = sorted(news_sensitive.iteritems(),key=lambda(k,v):v,reverse=True)
##    with open('./1219/sensitive_score_twitter.csv','wb')as f:
##        writer = csv.writer(f)
##        for k,v in news_sensitive.iteritems():
##            row = [k]
##            row.extend(v)
            #writer.writerow((row))
    #print sensitive_score

def read_sensitive_score(start_cal,end_cal,COLLECTION_NAME,sensitive_words):
    mongodb = conn[MONGODB_DB]
    collection = mongodb[COLLECTION_NAME]
    results = collection.find()
    news_sensitive = {}
    news_all = {}
    i = 0
    for li in results:
        try:
            timestamp = float(li["created_at"])
        except UnicodeError:
            timestamp = 'Null'
        if timestamp != 'Null':
            if float(li["created_at"]) >= start_cal and float(li["created_at"])<=end_cal:
                if li.has_key("sensitive_score"):
                    news_sensitive[li["_id"]] = [li["sensitive_score"],li["text"],li["keyword"],ts2HMS(float(li["created_at"])),li["screen_name"],li["user"]]
            #news_all[li["_id"]] = [li["text"],li["keyword"],ts2HMS(float(li["created_at"])),li["screen_name"],li["user"]]
    '''
    with open('./0102/sensitive_score_twitter.csv','wb')as f:
        writer = csv.writer(f)
        for k,v in news_sensitive.iteritems():
        #for k,v in news_all.iteritems():
            row = [k]
            row.extend(v)
            writer.writerow((row))
    '''
def main_yahoo(start_cal,end_cal,COLLECTION_NAME):
    '''
    输入数据：
    start_cal:计算开始时间，格式“年-月-日”
    end_cal:计算结束时间，格式“年-月-日”
    '''
    words = sensitive_words()
    read_news(HMS2ts(start_cal),HMS2ts(end_cal),COLLECTION_NAME,words)

def main_twitter(start_cal,end_cal,COLLECTION_NAME):
    '''
    输入数据：
    start_cal:计算开始时间，格式“年-月-日”
    end_cal:计算结束时间，格式“年-月-日”
    '''
    words = sensitive_words()
    read_twitter(HMS2ts(start_cal),HMS2ts(end_cal),COLLECTION_NAME,words)
    #read_sensitive_score(HMS2ts(start_cal),HMS2ts(end_cal),COLLECTION_NAME,words)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='abnormal.log',
                filemode='a')
    today = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=1)
    yesterday = today - datetime.timedelta(days=1)
    start_cal = str(yesterday)
    end_cal = str(tomorrow)
    start_time = time.time()
    logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='abnormal.log',
                filemode='a')
    logging.info('start cal sensitive_score_twitter :'+ str(ts2HMS_1(time.time()))  )
    main_twitter(start_cal,end_cal,MONGOD_COLLECTION_2)
    logging.info('end cal sensitive_score_twitter :'+ str(ts2HMS_1(time.time()))  )
    end_time = time.time()
    ca_time = end_time - start_time
    logging.info('total cal_time sensitive_score_twitter :'+ str(ca_time)  )
    print "sensitive_score_twitter cal_time:"+str(ca_time)
                    
    
