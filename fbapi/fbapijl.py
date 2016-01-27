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


MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
conn = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
MONGODB_DB = 'taiwan'



def main():
    mongodb = conn[MONGODB_DB]
    like_collection = ['tsai12','llchu12','song12']
    for collection in like_collection:
        col = mongodb[collection]
        results = col.find()
        f = open( collection +'.jl', 'ab')
        fline = ['comment_count','m_id','author','likes']
        for li in results:
            for flist in fline:
                try:
                    li[flist] = li[flist]
                except:
                    li[flist] = ''
            item = {
                'comment_count':li['comment_count'],
                'm_id':li['m_id'],
                'author':li['author'],
                'likes':li['likes']
            }
            lines = json.dumps(item)+'\n'
            f.write(lines)
        f.close()
    comment_collection = ['tsai11','llchu11','soong11']
    for collection in comment_collection:
        col = mongodb[collection]
        results = col.find()
        f = open( collection +'.jl', 'ab')
        fline = ['comment_count','m_id','author','comments']
        for li in results:
            for flist in fline:
                try:
                    li[flist] = li[flist]
                except:
                    li[flist] = ''
            item = {
                'comment_count':li['comment_count'],
                'm_id':li['m_id'],
                'author':li['author'],
                'comments':li['comments']
            }
            lines = json.dumps(item)+'\n'
            f.write(lines)
        f.close()
    post_collection = ['tsai','llchu','soong']
    for collection in post_collection:
        col = mongodb[collection]
        results = col.find()
        f = open( collection +'.jl', 'ab')
        fline = ['content','comment_count','m_id','author','per_favourity_count','time','created_time_timestamp','comment','like']
        for li in results:
            for flist in fline:
                try:
                    li[flist] = li[flist]
                except:
                    li[flist] = ''
            item = {
                'content':li['content'],
                'comment_count':li['comment_count'],
                'm_id':li['m_id'],
                'author':li['author'],
                'per_favourity_count':li['per_favourity_count'],
                'time':li['time'],
                'created_time_timestamp':li['created_time_timestamp'],
                'comment':li['comment'],
                'like':li['like']
            }
            lines = json.dumps(item)+'\n'
            f.write(lines)
        f.close()
    


if __name__ == '__main__':
    main()
