# -*- coding: cp936 -*-
# -*- coding：utf-8 -*-
from langconv import *
import os
import csv
import types
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import time
import datetime
import pymongo
from pymongo import MongoClient
import json
import logging

MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
conn = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
MONGODB_DB = 'taiwan'
MONGOD_COLLECTION_1 = 'fb_status_tsaiingwen'
MONGOD_COLLECTION_2 = 'fb_status_llchu'
MONGOD_COLLECTION_3 = 'fb_status_soong'
MONGOD_COLLECTION_4 = 'fb_status_chuchupepper'
MONGOD_COLLECTION_5 = 'tisr'
MONGOD_COLLECTION_6 = 'prediction_result'
MONGOD_COLLECTION_7 = 'fb_post_zan'

RATIO_20_40 = 0.3796
RATIO_40 = 0.1930
RATIO_50 = 0.1920
RATIO_60 = 0.1313
RATIO_70 = 0.1042
POLL_WEIGHT = 0
FB_WEIGHT = 0

def ts2HMS(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

def HMS2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d')))

def HMS2ts_1(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d %H:%M:%S')))

def ts2HMS_1(ts):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(ts))

def daily_favorite(start,end,COLLECTION_NAME,author):
    '''
    计算给定时间前30天以内候选人帖子赞数
    输入数据：
    end:当前时间节点
    COLLECTION_NAME:数据库名称
    author：collection名称
    输出数据：
    week_average:{"开始时间":平均赞数}
    favourite_count:{"_id":赞数}
    '''
    mongodb = conn[MONGODB_DB]
    collection = mongodb[COLLECTION_NAME]
    collection_zan = mongodb[MONGOD_COLLECTION_7]
    start = end-30*24*60*60
    week_average = {}
    results = collection.find({"author":author})
    #print author,results.count()
    daily_count_dict = {}#{"_id":{"日期"：赞数，“日期”：赞数,....}}
    zan_count = {}#{"_id":赞数}
    in_time_count = 0
    not_in_time_count = 0
    date = {}
    for li in results:
        if float(li["created_time_timestamp"])>=start and float(li["created_time_timestamp"])<=end:
            in_time_count += 1
            #daily_count_dict[li["_id"]] = li["daily_count"]
            if li.has_key("duplicate") ==True and li["duplicate"]== 'true':
                continue
            else:
                zan_count[li["_id"]] = li["per_favourity_count"]
                if type(li["daily_count"]) != dict:
                    for item in li["daily_count"].split('{')[1].split(','):
                        if "\'" in item.split(':')[0]:
                            try:
                                if '}' in item.split(':')[-1].split("\'")[1]:
                                    daily_count_dict[li["_id"]][str(item.split(':')[0].split('\'')[1].split(' ')[0])] = int(item.split(':')[-1].split("\'")[1].split('}')[0])
                                else:
                                    daily_count_dict[li["_id"]][str(item.split(':')[0].split('\'')[1].split(' ')[0])] = int(item.split(':')[-1].split("\'")[1])
                            except KeyError:
                                daily_count_dict[li["_id"]] = {}
                                if '}' in item.split(':')[-1].split("\'")[1]:
                                    daily_count_dict[li["_id"]][str(item.split(':')[0].split('\'')[1].split(' ')[0])] = int(item.split(':')[-1].split("\'")[1].split('}')[0])
                                else:
                                    daily_count_dict[li["_id"]][str(item.split(':')[0].split('\'')[1].split(' ')[0])] = int(item.split(':')[-1].split("\'")[1])
                        else:
                            if "\'" in item.split(':')[-1]:
                                try:
                                    if '}' in item.split(':')[-1].split('\'')[1]:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\'')[1].split('}')[0])
                                    else:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\'')[1])
                                except KeyError:
                                    daily_count_dict[li["_id"]] = {}
                                    if '}' in item.split(':')[-1].split('\'')[1]:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\'')[1].split('}')[0])
                                    else:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\'')[1])
                            else:
                                try:
                                    if '}' in item.split(':')[-1]:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('}')[0])
                                    else:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1])

                                except KeyError:
                                    daily_count_dict[li["_id"]] = {}
                                    if '}' in item.split(':')[-1]:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('}')[0])
                                    else:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1])
                else:
                    daily_count_dict[li["_id"]] = li["daily_count"]
                try:
                    date[ts2HMS(float(li["created_time_timestamp"]))][li["_id"]]=daily_count_dict[li["_id"]]
                except KeyError:
                    date[ts2HMS(float(li["created_time_timestamp"]))]={}
                    date[ts2HMS(float(li["created_time_timestamp"]))][li["_id"]]=daily_count_dict[li["_id"]]
            
        else:
            not_in_time_count += 1
        

    favourite_count = {}#{"_id":最接近统计时间节点的赞数}
    date_favourite_count = {}#{"date":{"_id":{"时间"：赞数}}}
    for k,v in daily_count_dict.iteritems():
        everyday_count = {}#每条帖子每天的赞数
##        print k,v,type(v)
        for key, value in v.iteritems():
            if ':' in key:
                time_key = HMS2ts_1(key)
            else:
                time_key = HMS2ts(key.strip("\"")[0:10])
            if (time_key >=start) and (time_key<=end):
                everyday_count[time_key]=value
        #找到时间区间内最接近统计时间节点的记录
        if len(everyday_count) == 0:
            zan = zan_count[k]
        else:
            zan = everyday_count[max(everyday_count.keys())]
        favourite_count[k] = float(zan)

    for k,v in date.iteritems():
        for key,value in v.iteritems():
            try:
                date_favourite_count[k][key]=favourite_count[key]
            except KeyError:
                date_favourite_count[k] = {}
                date_favourite_count[k][key]=favourite_count[key]

    average_date_favourite_count = {}
    for k,v in date_favourite_count.iteritems():
        daily_count = []
        for key,value in v.iteritems():
            daily_count.append(value)
        average = float(sum(daily_count))/float(len(daily_count))
        average_date_favourite_count[k] = [average,sum(daily_count),len(daily_count)]
        #if collection_zan.find({'date':})
        #print average,sum(daily_count),len(daily_count)
    for k,v in average_date_favourite_count.iteritems():
        row = [k]
        row.extend(v)
        if collection_zan.find({'date':row[0],'author':author}).count():
            collection_zan.update({'date':row[0],'author':author},{'$set':{'average_daily_zan':row[1],'daily_total_zan':row[2],'daily_post':row[3]}})
        else:
            collection_zan.insert({'date':row[0],'author':author,'average_daily_zan':row[1],'daily_total_zan':row[2],'daily_post':row[3]})
        #print type(row),author
    '''    
    with open('./0102/zan_%s.csv'%author,'wb')as f:
        writer = csv.writer(f)
        for k,v in favourite_count.iteritems():
            row = [k,v]
            writer.writerow((row))
    with open('./0102/average_daily_zan_%s.csv'%author,'wb')as f:
        writer = csv.writer(f)
        for k,v in average_date_favourite_count.iteritems():
            row = [k]
            row.extend(v)
            writer.writerow((row))
    '''
def output_fb(start,end,COLLECTION_NAME,author):
    mongodb = conn[MONGODB_DB]
    collection = mongodb[COLLECTION_NAME]
    results = collection.find({"author":author})
    output = {}
    for li in results:
        try:
            timestamp = float(li["created_time_timestamp"])
        except UnicodeError:
            timestamp = 'Null'
        if timestamp != 'Null':
            if float(li["created_time_timestamp"])>=start and float(li["created_time_timestamp"])<=end:
                output[li["_id"]] = [li["content"],ts2HMS(float(li["created_time_timestamp"])),li["per_favourity_count"],li["daily_count"]]
    with open('./0102/fb_%s.csv'%author,'wb')as f:
        writer = csv.writer(f)
        for k,v in output.iteritems():
            row = [k]
            row.extend(v)
            writer.writerow((row))
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='facebook.log',
                filemode='a')
    today = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=1)
    yesterday = today - datetime.timedelta(days=1)
    #start = str(yesterday)
    start = '2015-12-30'
    end = str(tomorrow)
    logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='facebook.log',
                filemode='a')
    logging.info(' start cal fb_post_zan:' + str(ts2HMS_1(time.time())) )
    daily_favorite(HMS2ts(start),HMS2ts(end),MONGOD_COLLECTION_1,"tsaiingwen")
    daily_favorite(HMS2ts(start),HMS2ts(end),MONGOD_COLLECTION_2,"llchu")
    daily_favorite(HMS2ts(start),HMS2ts(end),MONGOD_COLLECTION_3,"soong")
    logging.info(' end cal fb_post_zan:' + str(ts2HMS_1(time.time())) )
##    output_fb(HMS2ts(start),HMS2ts(end),MONGOD_COLLECTION_1,"tsaiingwen")
##    output_fb(HMS2ts(start),HMS2ts(end),MONGOD_COLLECTION_2,"llchu")
##    output_fb(HMS2ts(start),HMS2ts(end),MONGOD_COLLECTION_3,"soong")
    
