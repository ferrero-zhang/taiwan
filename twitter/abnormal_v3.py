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
import json
import numpy as np
import math
import logging

MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
conn = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
MONGODB_DB = 'taiwan'
MONGOD_COLLECTION = 'twitter'
MONGOD_COLLECTION_1 = 'abnormal'
mongodb = conn[MONGODB_DB]
collection1 = mongodb['days']
DAYS = collection1.find().sort('timestamp',pymongo.DESCENDING)
NUM = []
for li in DAYS:
    NUM.append(li['abnormal_day'])
DAY_NUM = NUM[0]
#print DAY_NUM
today = datetime.date.today()
tomorrow = today + datetime.timedelta(days=1)
yesterday = today - datetime.timedelta(days=1)
last_week = today - (datetime.timedelta(days=1))*8


def ts2HMS(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

def HMS2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d')))

def ts2HMS_1(ts):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(ts))

count={}

def keyword_list():
    '''
    读取数据库中的关键词
    '''
    mongodb = conn[MONGODB_DB]
    collection = mongodb[MONGOD_COLLECTION]
    results = collection.distinct("keyword")

    with open('./keyword_list.csv','wb')as f:
        writer = csv.writer(f)
        for item in results:
            writer.writerow(([item])) 

def read_mongo():
    mongodb = conn[MONGODB_DB]
    collection = mongodb[MONGOD_COLLECTION]
    collection_count = mongodb[MONGOD_COLLECTION_1]
    i = 0
    kw_tsai = []
    kw_llchu = []
    kw_soong = []
    with open('./keyword_list.csv','rb')as f:
        reader = csv.reader(f)
        for row in reader:
            if i <=3:
                kw_tsai.append(row[0].strip())
            elif i<=10:
                kw_llchu.append(row[0].strip())
            elif i <=14:
                kw_soong.append(row[0].strip())
            i += 1

    start = str(yesterday)
    end = str(tomorrow)
    day = (HMS2ts(end)-HMS2ts(start))/(24*60*60)

    results_tsai = collection.find({"keyword":{"$in":kw_tsai}})
    results_llchu = collection.find({"keyword":{"$in":kw_llchu}})
    results_soong = collection.find({"keyword":{"$in":kw_soong}})
    count_tsai = {}
    count_llchu = {}
    count_soong = {}
    for li in results_tsai:
        try:
            timestamp_tsai = float(li["created_at"])
            if timestamp_tsai >= HMS2ts(start) and timestamp_tsai<=HMS2ts(end):
                try:
                    count_tsai[ts2HMS(timestamp_tsai)] += 1
                except KeyError:
                    count_tsai[ts2HMS(timestamp_tsai)] = 1
        except UnicodeEncodeError:
            print li["_id"]

    for li in results_llchu:
        try:
            timestamp_llchu = float(li["created_at"])
            if timestamp_llchu >= HMS2ts(start) and timestamp_llchu<=HMS2ts(end):
                try:
                    count_llchu[ts2HMS(timestamp_llchu)] += 1
                except KeyError:
                    count_llchu[ts2HMS(timestamp_llchu)] = 1
        except UnicodeEncodeError:
            print li["_id"]
    
    for li in results_soong:
        try:
            timestamp_soong = float(li["created_at"])
            if timestamp_soong >= HMS2ts(start) and timestamp_soong<=HMS2ts(end):
                try:
                    count_soong[ts2HMS(timestamp_soong)] += 1
                except KeyError:
                    count_soong[ts2HMS(timestamp_soong)] = 1
        except UnicodeEncodeError:
            print li["_id"]        

    for i in range(day):
        date = ts2HMS(HMS2ts(start)+i*24*60*60)
        if count_tsai.has_key(date):
            tsai = count_tsai[date]
        else:
            tsai = 0

        if count_llchu.has_key(date):
            llchu = count_llchu[date]
        else:
            llchu = 0

        if count_soong.has_key(date):
            soong = count_soong[date]
        else:
            soong = 0
        if collection_count.find({"date":date}).count():
            collection_count.update({"date":date},{'$set':{"tsai":tsai,"llchu":llchu,"soong":soong,"tsai_lower_bound":0,"llchu_lower_bound":0,"soong_lower_bound":0}})
        else:
            collection_count.insert({"date":date,"tsai":tsai,"llchu":llchu,"soong":soong,"tsai_lower_bound":0,"llchu_lower_bound":0,"soong_lower_bound":0})
        print date,count_tsai,count_llchu,count_soong

def read_mongo_today():
    start_cal = HMS2ts(datetime.date.today().strftime("%Y-%m-%d"))
    print start_cal
    end_cal = time.time()
    mongodb = conn[MONGODB_DB]
    collection = mongodb[MONGOD_COLLECTION]
    collection_count = mongodb[MONGOD_COLLECTION_1]
    i = 0
    kw_tsai = []
    kw_llchu = []
    kw_soong = []
    with open('./keyword_list.csv','rb')as f:
        reader = csv.reader(f)
        for row in reader:
            if i <=3:
                kw_tsai.append(row[0].strip())
            elif i<=10:
                kw_llchu.append(row[0].strip())
            elif i <=14:
                kw_soong.append(row[0].strip())
            i += 1

    results_tsai = collection.find({"keyword":{"$in":kw_tsai}})
    results_llchu = collection.find({"keyword":{"$in":kw_llchu}})
    results_soong = collection.find({"keyword":{"$in":kw_soong}})
    count_tsai = 0
    count_llchu = 0
    count_soong = 0
    for li in results_tsai:
        try:
            timestamp_tsai = float(li["created_at"])
            if (timestamp_tsai>=start_cal and timestamp_tsai<=end_cal):
                count_tsai += 1
        except UnicodeEncodeError:
            print li["_id"]

    for li in results_llchu:
        try:
            timestamp_llchu = float(li["created_at"])
            if (timestamp_llchu>=start_cal and timestamp_llchu<=end_cal):
                count_llchu += 1
        except UnicodeEncodeError:
            print li["_id"]
                
                
    for li in results_soong:
        try:
            timestamp_soong = float(li["created_at"])
            if (timestamp_soong>=start_cal and timestamp_soong<=end_cal):
                count_soong += 1
        except UnicodeEncodeError:
            print li["_id"]
    collection_count.insert({"date":ts2HMS(start_cal),"tsai":count_tsai,"llchu":count_llchu,"soong":count_soong,"tsai_lower_bound":0,"llchu_lower_bound":0,"soong_lower_bound":0})
    print ts2HMS(start_cal),count_tsai,count_llchu,count_soong

def confidence_interval(now_day,day_num):
    '''
    给出下一天tweet数据量的置信区间
    输入数据:
    tweet_count:{"日期"：[蔡，朱，宋tweet数量]}
    输出数据：
    [tsai_lower_bound, tsai_upper_bound, llchu_lower_bound, llchu_upper_bound,soong_lower_bound,soong_upperbound]:三位参选人置信区间的上下界
    '''
    tsai={}
    llchu={}
    soong={}
    start_day = HMS2ts(now_day)-day_num*24*60*60

    mongodb = conn[MONGODB_DB]
    collection_count = mongodb[MONGOD_COLLECTION_1]
    results = collection_count.find()
    for li in results:
        #print li["date"],type(li["date"])
        if HMS2ts(li["date"]) >= start_day and HMS2ts(li["date"]) <= now_day:
            #不剔除低的异常点
            tsai[li["date"]] = li["tsai"]
            llchu[li["date"]] = li["llchu"]
            soong[li["date"]] = li["soong"]
##            if float(li["tsai"])>=float(li["tsai_lower_bound"]):
##                tsai[ts2HMS(li["date"])] = li["tsai"]
##            if float(li["llchu"]) >= float(li["llchu_lower_bound"]):
##                llchu[ts2HMS(li["date"])] = li["llchu"]
##            if float(li["soong"])>= float(li["soong_lower_bound"]):
##                soong[ts2HMS(li["date"])] = li["soong"]

    mean_tsai = np.mean(tsai.values())
    std_tsai = np.std(tsai.values())
    #print tsai
    tsai_lower_bound = mean_tsai - (1.95*std_tsai)/math.sqrt(day_num)
    tsai_upper_bound = mean_tsai + (1.95*std_tsai)/math.sqrt(day_num)

    mean_llchu = np.mean(llchu.values())
    std_llchu = np.std(llchu.values())
    llchu_lower_bound = mean_llchu - (1.95*std_llchu)/math.sqrt(day_num)
    llchu_upper_bound = mean_llchu + (1.95*std_llchu)/math.sqrt(day_num)

    mean_soong = np.mean(soong.values())
    std_soong = np.std(soong.values())
    soong_lower_bound = mean_soong - (1.95*std_soong)/math.sqrt(day_num)
    soong_upper_bound = mean_soong + (1.95*std_soong)/math.sqrt(day_num)

    mongodb = conn[MONGODB_DB]
    collection_count = mongodb[MONGOD_COLLECTION_1]
    tweet_count = collection_count.find({"date":now_day})
    print tsai_upper_bound, tsai_lower_bound
    updates_modifier = {'$set':{"tsai_upper_bound":tsai_upper_bound,"tsai_lower_bound":tsai_lower_bound,"llchu_upper_bound":llchu_upper_bound,"llchu_lower_bound":llchu_lower_bound,"soong_upper_bound":soong_upper_bound,"soong_lower_bound":soong_lower_bound}}
    for li in tweet_count:
        collection_count.update({"_id":li["_id"]},updates_modifier)    
    #return [tsai_lower_bound, tsai_upper_bound, llchu_lower_bound, llchu_upper_bound,soong_lower_bound,soong_upper_bound],[mean_tsai,std_tsai,mean_llchu,std_llchu,mean_soong,std_soong]

def find_all_abnormal():
    '''
    找到数据库中的全部异常点
    '''
    mongodb = conn[MONGODB_DB]
    collection_count = mongodb[MONGOD_COLLECTION_1]
    results = collection_count.find()
    for li in results:
        tsai_count = li["tsai"]
        tsai_upper = li["tsai_upper_bound"]
        tsai_lower = li["tsai_lower_bound"]

        llchu_count = li["llchu"]
        llchu_upper = li["llchu_upper_bound"]
        llchu_lower = li["llchu_lower_bound"]

        soong_count = li["soong"]
        soong_upper = li["soong_upper_bound"]
        soong_lower = li["soong_lower_bound"]

        if float(tsai_count) >= tsai_upper:
            tsai_abnormal = 1
        elif float(tsai_count) <= tsai_lower:
            tsai_abnormal = -1
        else:
            tsai_abnormal = 0

        if float(llchu_count) >= llchu_upper:
            llchu_abnormal = 1
        elif float(llchu_count) <= llchu_lower:
            llchu_abnormal = -1
        else:
            llchu_abnormal = 0

        if float(soong_count) >= soong_upper:
            soong_abnormal = 1
        elif float(soong_count) <= soong_lower:
            soong_abnormal = -1
        else:
            soong_abnormal = 0

        update_modifier = {'$set':{"tsai_abnormal":tsai_abnormal,"llchu_abnormal":llchu_abnormal,"soong_abnormal":soong_abnormal}}
        collection_count.update({"_id":li["_id"]},update_modifier)

def find_abnormal_everyday(today):
    '''
    判断当天是否是异常点
    '''
    mongodb = conn[MONGODB_DB]
    collection_count = mongodb[MONGOD_COLLECTION_1]
    results_today = collection_count.find({"date":today})
    for li in results_today:
        tsai_count = li["tsai"]
        tsai_upper = li["tsai_upper_bound"]
        tsai_lower = li["tsai_lower_bound"]

        llchu_count = li["llchu"]
        llchu_upper = li["llchu_upper_bound"]
        llchu_lower = li["llchu_lower_bound"]

        soong_count = li["soong"]
        soong_upper = li["soong_upper_bound"]
        soong_lower = li["soong_lower_bound"]

        if float(tsai_count) >= tsai_upper:
            tsai_abnormal = 1
        elif float(tsai_count) <= tsai_lower:
            tsai_abnormal = -1
        else:
            tsai_abnormal = 0

        if float(llchu_count) >= llchu_upper:
            llchu_abnormal = 1
        elif float(llchu_count) <= llchu_lower:
            llchu_abnormal = -1
        else:
            llchu_abnormal = 0

        if float(soong_count) >= soong_upper:
            soong_abnormal = 1
        elif float(soong_count) <= soong_lower:
            soong_abnormal = -1
        else:
            soong_abnormal = 0

        update_modifier = {'$set':{"tsai_abnormal":tsai_abnormal,"llchu_abnormal":llchu_abnormal,"soong_abnormal":soong_abnormal}}
        collection_count.update({"_id":li["_id"]},update_modifier)    

def main(daynum):
    '''
    实际运行后，每小时执行一次该函数
    输入数据：
    daynum:异常检测中，移动平均选择的天数
    '''
    today = HMS2ts(datetime.date.today().strftime("%Y-%m-%d"))
    #read_mongo_today()
    mongodb = conn[MONGODB_DB]
    collection_count = mongodb[MONGOD_COLLECTION_1]
    confidence_interval(ts2HMS(today),daynum)
    find_abnormal_everyday(ts2HMS(today))
    

def test_main(start,end,DAY_NUM):
    '''
    首次运行时，使用该函数
    给定开始时间和结束时间，可以计算一段时间内的异常点
    输入数据:
    start：开始日期，格式“年-月-日”
    end：结束日期，格式“年-月-日”
    '''
    start_time = time.time()
##    start = '2015-10-1'
##    end = '2015-12-13'
    day = (HMS2ts(end)-HMS2ts(start))/(24*60*60)
    read_mongo()#每天的tweet量
    mongodb = conn[MONGODB_DB]
    collection_count = mongodb[MONGOD_COLLECTION_1]
    for i in range(day):
        print ts2HMS(HMS2ts(start)+i*24*60*60)
        confidence_interval(ts2HMS(HMS2ts(start)+i*24*60*60),DAY_NUM)
        find_abnormal_everyday(ts2HMS(HMS2ts(start)+i*24*60*60)) 

    result_tsai = {}
    result_llchu = {}
    result_soong = {}
    results = collection_count.find()
    for li in results:
        if HMS2ts(li["date"]) >= HMS2ts(start) and HMS2ts(li["date"]) <= HMS2ts(end):
            result_tsai[li["date"]] = [li["tsai"],li["tsai_upper_bound"],li["tsai_lower_bound"],li["tsai_abnormal"]]
            result_llchu[li["date"]] = [li["llchu"],li["llchu_upper_bound"],li["llchu_lower_bound"],li["llchu_abnormal"]]
            result_soong[li["date"]] = [li["soong"],li["soong_upper_bound"],li["soong_lower_bound"],li["soong_abnormal"]]

    with open('./result_tsai_'+ str(today) +'.csv','wb')as f:
        writer = csv.writer(f)
        for k,v in result_tsai.iteritems():
            row = [k]
            row.extend(v)
            writer.writerow((row))

    with open('./result_llchu_'+ str(today) +'.csv','wb')as f:
        writer = csv.writer(f)
        for k,v in result_llchu.iteritems():
            row = [k]
            row.extend(v)
            writer.writerow((row))

    with open('./result_soong_'+ str(today) +'.csv','wb')as f:
        writer = csv.writer(f)
        for k,v in result_soong.iteritems():
            row = [k]
            row.extend(v)
            writer.writerow((row))


if __name__ == '__main__':
    #read_mongo()
    logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='abnormal.log',
                filemode='a')
    #start = str(today)
    start = '2016-01-01'
    end = str(tomorrow)
    #print tomorrow
    start_time = time.time()
    logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='abnormal.log',
                filemode='a')
    logging.info('start cal abnormal :'+ str(ts2HMS_1(time.time()))  )
    test_main(start,end,DAY_NUM)
    end_time = time.time()
    logging.info('end cal abnormal :'+ str(ts2HMS_1(time.time()))  )
    cal_time = float(end_time)-float(start_time)
    logging.info('total cal_time abnormal :'+ str(cal_time)  )
    print "cal_time:"+str(cal_time)
    #main(DAY_NUM)

