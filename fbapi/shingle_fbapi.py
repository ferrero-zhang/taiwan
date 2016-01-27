# -*- coding: utf-8 -*-

import urllib2
from urllib2 import urlopen
from simplejson import loads
import time
import csv
import json
import pymongo
from pymongo import MongoClient
MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
conn = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
MONGODB_DB = 'taiwan'
MONGOD_COLLECTION = 'tsai'
MONGOD_COLLECTION_1 = 'tsai11'
MONGOD_COLLECTION_2 = 'tsai12'
mongodb = conn[MONGODB_DB]
collection = mongodb[MONGOD_COLLECTION]
collection1 = mongodb[MONGOD_COLLECTION_1]
collection2 = mongodb[MONGOD_COLLECTION_2]

def main():
    results = collection.find()
    like = []
    comment = []
    for li in results:
        likes = collection2.find({'m_id':li['m_id']})
        for i in likes:
            like = i['likes']  
        comments = collection1.find({'m_id':li['m_id']})
        for j in comments:
            comment = j['comments']
        li['like'] = like
        li['comment'] = comment 
        collection.update({'m_id':li['m_id']},{'$set':li})

if __name__ == '__main__':
    main()
