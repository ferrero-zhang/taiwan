# -*- coding: utf-8 -*-

import csv
import time
import datetime
import pymongo
from pymongo import MongoClient


MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
conn = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
MONGODB_DB = 'taiwan'
MONGOD_COLLECTION = 'tsai'


def ts2HMS(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

def HMS2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d')))

def HMS2ts_1(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d %H:%M:%S')))

def read_mid(start,end):
    mongodb = conn[MONGODB_DB]
    collection = mongodb[MONGOD_COLLECTION]
    
    f = open('tsai06.csv', 'wb')
    writer = csv.writer(f)
    #writer.writerow(['date','total_count','tsai','llchu','soong','per_tsai','per_llchu','per_soong'])
    results = collection.find({'created_time_timestamp':{'$gte':start,'$lte':end}})
    for li in results:
        linelist = [li['m_id']]
        writer.writerow(linelist)
    f.close()
            

if __name__ == '__main__':
    start = '2015-6-1'
    today = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=1)
    end = '2015-7-1'
    read_mid(HMS2ts(start),HMS2ts(end))
    
    
