# -*- coding: utf-8 -*-
#from langconv import *
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
MONGOD_COLLECTION_1 = 'yahoo'
MONGOD_COLLECTION_2 = 'twitter'
MONGOD_COLLECTION_3 = 'news'

def ts2HMS(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

def HMS2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d')))

def ts2HMS_1(ts):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(ts))


def news_importance(start_cal,end_cal,COLLECTION_NAME):
    '''
    读入给定时间段内的新闻，计算重要度（重复新闻越多，则认为该新闻越重要）
    输入数据：
    start_cal:开始计算日期,格式为“年-月-日”
    end_cal:结束日期
    '''
    mongodb = conn[MONGODB_DB]
    collection = mongodb[COLLECTION_NAME]
    results = collection.find()
    news_importance = {}
    i = 0
    news_reference = {}
    news_content={}
    importance_score = {}
    for li in results:
        i += 1
        #print i
        try:
            timestamp = float(li["timestamp"])
        except UnicodeError:
            timestamp = 'Null'
        except ValueError:
            timestamp = 'Null'
            #print li["timestamp"]
        if timestamp != 'Null':
            if timestamp>= HMS2ts(start_cal) and timestamp<=HMS2ts(end_cal):
                if li.has_key("duplicate") and li["duplicate"] == 'true':
                    try :
                        news_importance[li["refer"]].append(li["_id"])
                    except KeyError:
                        news_importance[li["refer"]] = []
                        news_importance[li["refer"]].append(li["_id"])
                        news_content[li["refer"]] = [li["title"],li['summary'],ts2HMS(float(timestamp)),li["source"],li["keyword"]]
    for k,v in news_importance.iteritems():
        importance_score[k] = len(v)

##    print importance_score
    sorted_news = sorted(importance_score.iteritems(),key=lambda(k,v):v,reverse=True)
##    print sorted_news
    
    for item in sorted_news:
        if news_content.has_key(item[0]):
            row = [item[0],item[1]]
            row.extend(news_content[item[0]])
        else:
            row = [item[0],item[1]]
        collection.update({'_id':row[0]},{'$set':{'importance':row[1]}})

##            row.append(news_content[item[0]])
##            else:row.append("NULL")
##            row.extend(news_importance[item[0]])

            
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='news.log',
                filemode='a')
    today = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=1)
    yesterday = today - datetime.timedelta(days=1)
    start_cal = str(yesterday)
    end_cal = str(tomorrow)
    print 'news calculation begins....'
    start_time = time.time()
    logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='news.log',
                filemode='a')
    logging.info(' start importance_score_news:' + str(ts2HMS_1(time.time())) )
    news_importance(start_cal, end_cal,MONGOD_COLLECTION_3)
    logging.info(' end importance_score_news:' + str(ts2HMS_1(time.time())) )
    end_time = time.time()
    cal_time = end_time - start_time
    logging.info('total cal_time importance_score_news :'+ str(cal_time)  )
    print "importance_score_news cal_time:"+str(cal_time)
