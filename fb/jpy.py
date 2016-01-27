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
MONGOD_COLLECTION = 'tsaiingwen'
mongodb = conn[MONGODB_DB]
collection = mongodb[MONGOD_COLLECTION]
ACCESS_TOKEN = 'CAAPaGxj8KykBAJT0xZARYV3rAg8eJTH0gKywaSf1ZC2zFoAXBsM8UZCqtPoWNRlvYTIXfrf2KXwih1X3WaPUazomGN9TA61CIlvQWg1iw7OEbrxRGGuI6ABTgNA6HmaylzQyU3OPlQQVeLnM5aK5c7zCt6f1z48rUmJYus6hA83sPNmsYca'
localtime =  time.strftime("%Y-%m-%d ",time.localtime(time.time()))

def main():
    req = urllib2.Request('https://graph.facebook.com/v2.5/46251501064_10153123962676065/comments?limit=1000summary=1&access_token='+ACCESS_TOKEN)
    content = loads(urlopen(req).read())
    message_id = '46251501064_10153123962676065'
    comment_count = content['summary']['total_count']
    comments_info = []
    comments = content['data']
    for li in  comments:
        try:
            comment = li['from']
            user_name = comment['name']
            user_id = comment['id']
        except:
            continue
        comment_message = li['message']
        created_time = li['created_time']
        created_time_timestamp = time2stamp(created_time) 
        comment = {
            'user_name':user_name,
            'user_id':user_id,
            'comment_message':comment_message,
            'created_time':created_time,
            'created_time_timestamp':created_time_timestamp
        }
        
        comments_info.append(comment)
    
    next_page = content['paging']['next']
    comment = next(next_page)
    for i in range(len(comment)):
        print comment[i]
        comments_info.append(comment[i])
    
    item = {
        'm_id':message_id,
        'comment_count':comment_count,
        'author':'tsai',
        'comments':comments_info
    }
        
    process_item(item)

def process_item(item):
    if collection.find({'m_id':item['m_id']}).count():
        updates_modifier = {'$set':item}
        collection.update({'m_id':item['m_id']},updates_modifier)
        #collection.update({'m_id':item['m_id']},{'$set':{'like_user':item['like_user'],'comment_user':item['comment_user']}})
    else:
        collection.insert({'m_id':item['m_id'],'author':item['author'],'comment_count':item['comment_count'],'comment':item['comments']})






def get_name(user_id):
    reqs = urllib2.Request("https://graph.facebook.com/v2.5/"+ str(user_id)+"?access_token="+ACCESS_TOKEN)
    con = loads(urlopen(reqs).read())
    if isinstance(con['name'],unicode):
        name = con['name'].encode('utf-8')
    else:
        name = con['name']
    return name



def next(next_page):
    comment_info = []
    reqs = urllib2.Request(next_page)
    con = loads(urlopen(reqs).read())
    comments = con['data']
    for li in comments:
        #print li
        try:
            users = li['from']
            user_id = users['id']
            user_name = get_name(user_id)
        except:
            continue
        comment_message = li['message']
        comment_created = li['created_time']
        comment_stamp = time2stamp(comment_created)
        comment_item = {
            'user_id':user_id,
            'user_name':user_name,
            'comment_message':comment_message,
            'comment_created':comment_created,
            'comment_stamp':comment_stamp
        }
        comment_info.append(comment_item)
    try:
        next_page = con['paging']['next']
        more_user = next(next_page)
        for i in range(0,len(more_user)):
            print more_user[i]
            comment_info.append(more_user[i])
    except Exception,e:
        print e
    return comment_info

def time2stamp(timestr):
    yyddmm = timestr.split('T')[0]
    hhmmss = timestr.split('T')[1].split('+')[0]
    datestr = yyddmm + ' '+hhmmss
    timestamp = int(time.mktime(time.strptime(datestr,'%Y-%m-%d %H:%M:%S')))
    return timestamp

if __name__ == '__main__':
    main()
