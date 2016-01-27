# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import pymongo
import os
from pymongo import MongoClient
import json
import time
import csv
import datetime

#MONGODB_HOST = '113.10.156.125'
MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
conn = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
MONGODB_DB = 'taiwan'
MONGOD_COLLECTION = 'twitter'


def main():
    mongodb = conn[MONGODB_DB]
    collection = mongodb[MONGOD_COLLECTION]
    localtime =  time.strftime("%Y-%m-%d ",time.localtime(time.time()))
    now_time = datetime.datetime.now()
    yes_time = now_time + datetime.timedelta(days=-1)
    yes_time_nyr = yes_time.strftime('%Y-%m-%d')
    yes_time_stamp = time.mktime(time.strptime(yes_time_nyr, '%Y-%m-%d'))
    #print yes_time_stamp
    #flocal = localtime.replace(',','-')
    results = collection.find({ "created_at" : { '$gt': yes_time_stamp } } )
    print collection.find({ "created_at" : { '$gt': yes_time_stamp} } ).count()
    f = open( localtime +'.csv', 'wb')
    writer = csv.writer(f)
    i=0
    writer.writerow(["_id","lang","favourites_count","screen_name","candidate","keyword","first_in","text","created_at","last_modify","user","id_tweet","retweet_count","duplicate","refer"])
    flist = ["_id","lang","favourites_count","screen_name","candidate","keyword","first_in","text","created_at","last_modify","user","id_tweet","retweet_count","duplicate","refer"]
    for li in results:
        i += 1
        linelist = []
        for f in flist:
            try:
                fvalue = li[f]
            except KeyError,e:
                fvalue = ''
            linelist.append(fvalue)
        writer.writerow(linelist)
        print i
    f.close()
    


if __name__ == '__main__':
    main()
