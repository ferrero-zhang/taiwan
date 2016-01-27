# -*- coding: utf-8 -*-

import urllib2
from urllib2 import urlopen
from simplejson import loads
import time
import csv
import json
import pymongo
from pymongo import MongoClient
from setting import ACCESS_TOKEN
MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
conn = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
MONGODB_DB = 'taiwan'
MONGOD_COLLECTION = 'llchu12'
mongodb = conn[MONGODB_DB]
collection = mongodb[MONGOD_COLLECTION]
localtime =  time.strftime("%Y-%m-%d ",time.localtime(time.time()))

def main(m_id):
    req = urllib2.Request('https://graph.facebook.com/v2.3/'+ str(m_id) +'/likes?limit=1000&summary=1&access_token='+ACCESS_TOKEN)
    content = loads(urlopen(req).read())
    message_id = m_id
    like_count = content['summary']['total_count']
    likes_info = []
    comments = content['data']
    for li in  comments:
        
        user_id = li['id']
        #user_name = get_name(user_id)
        like = {
            #'user_name':user_name,
            'user_id':user_id
        }
        
        likes_info.append(like)
    
    item = {
        'm_id':message_id,
        'like_count':like_count,
        'author':'llchu',
        'likes':likes_info
    }
    process_item(item)
    try:
        next_page = content['paging']['next']
        more_comment = nextLike(next_page,message_id,like_count)
    except:
        print "aa"
    
    


def process_item(item):
    if collection.find({'m_id':item['m_id']}).count():
        print "update"
        result = collection.find({'m_id':item['m_id']})
        origin_like = result[0]['likes']
        for i in range(len(item['likes'])):
            origin_like.append(item['likes'][i])
        collection.update({'m_id':item['m_id']},{'likes':origin_like,'m_id':item['m_id'],'author':item['author'],'like_count':item['like_count']})
        
    else:
        collection.insert({'m_id':item['m_id'],'author':item['author'],'like_count':item['like_count'],'likes':item['likes']})
        
def get_name(user_id):
    reqs = urllib2.Request("https://graph.facebook.com/v2.5/"+ str(user_id)+"?access_token="+ACCESS_TOKEN)
    con = loads(urlopen(reqs).read())
    if isinstance(con['name'],unicode):
        name = con['name'].encode('utf-8')
    else:
        name = con['name']
    return name


def nextLike(next_page,message_id,like_count):

    reqs = urllib2.Request(next_page)
    con = loads(urlopen(reqs).read())
    comments = con['data']
    likes_info = []
    for li in  comments:
        user_id = li['id']
        #user_name = get_name(user_id)
        like = {
            #'user_name':user_name,
            'user_id':user_id
        }
        
        likes_info.append(like)
    
    item = {
        'm_id':message_id,
        'like_count':like_count,
        'author':'llchu',
        'likes':likes_info
    }
    process_item(item)
    try:
        next_page = con['paging']['next']
        more_user = nextLike(next_page,message_id,like_count)
    except Exception,e:
        print e
    return True

def time2stamp(timestr):
    yyddmm = timestr.split('T')[0]
    hhmmss = timestr.split('T')[1].split('+')[0]
    datestr = yyddmm + ' '+hhmmss
    timestamp = int(time.mktime(time.strptime(datestr,'%Y-%m-%d %H:%M:%S')))
    return timestamp

if __name__ == '__main__':
    f = open('llchu11.csv', 'rb')
    reader = csv.reader(f)
    for li in reader:
        print li[0]
        main(li[0])
