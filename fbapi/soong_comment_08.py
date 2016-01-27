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
MONGOD_COLLECTION = 'soong11'
mongodb = conn[MONGODB_DB]
collection = mongodb[MONGOD_COLLECTION]
ACCESS_TOKEN = 'CAAPaGxj8KykBAPj8MBjfCE1HqoIhfDHRFtss9p7Nd7dforcZBrVOg5chrknuRzi2ZAkJuHlQeqkohOGpMcTCHlcaGrKUwIJhSG4GYiOAiZAt5NuAtCMhGBD0MPwNVeSOjlvV5RNL0XpZC3WGEm0ZCJXnrwZBpCp8TrdBp48hkOiD52zaOQhhVY'
localtime =  time.strftime("%Y-%m-%d ",time.localtime(time.time()))

def main(m_id):
    req = urllib2.Request('https://graph.facebook.com/v2.5/'+ str(m_id) +'/comments?limit=1000&summary=1&access_token='+ACCESS_TOKEN)
    content = loads(urlopen(req).read())
    message_id = m_id
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
        #print comment
        comments_info.append(comment)
    
    item = {
        'm_id':message_id,
        'comment_count':comment_count,
        'author':'soong',
        'comments':comments_info
    }
    process_item(item)
    try:
        next_page = content['paging']['next']
        more_comment = nextComment(next_page,message_id,comment_count)
    except:
        print "aa"
    
    

def get_post(user_id):
    user_id = user_id
    timestore = []
    msm = []
    req_post = urllib2.Request("https://graph.facebook.com/v2.5/"+str(user_id)+"/posts?limit=100&since=2016-01-15&until=2016-01-17&access_token="+ACCESS_TOKEN)
    #print req_post
    con = loads(urlopen(req_post).read())
    posts = con['data']
    for li in posts:
        message_id = li['id']
        try:
            messages = li['message']
        except:
            messages = ''
        message_created = li['created_time']
        timestamp = time2stamp(message_created)
        likes_count = get_like(message_id)
        comment_count = get_comment(message_id)
        #like_users = get_likeuser(message_id)
        comment_users = get_commentuser(message_id)
        #print timestamp
        #print like_users
        #print comment_user
        item = {
            'm_id':message_id,
            'content':messages,
            'time':message_created,
            'created_time_timestamp':timestamp,
            'per_favourity_count':likes_count,
            'comment_count':comment_count,
            'author':'soong',
            #'like_user':like_users,
            'comment_user':comment_users
        }
        process_item(item)
        msm.append(item)
        timestore.append(item['created_time_timestamp'])
    print len(timestore)
    try:
        endstamp = timestore[int(len(timestore)-1)]
        post = get_post(user_id,endstamp)
    except:
        print "end"
    return msm

def process_item(item):
    if collection.find({'m_id':item['m_id']}).count():
        print "update"
        result = collection.find({'m_id':item['m_id']})
        origin_comment = result[0]['comments']
        for i in range(len(item['comments'])):
            origin_comment.append(item['comments'][i])
        collection.update({'m_id':item['m_id']},{'comments':origin_comment,'m_id':item['m_id'],'author':item['author'],'comment_count':item['comment_count']})
        
    else:
        collection.insert({'m_id':item['m_id'],'author':item['author'],'comment_count':item['comment_count'],'comments':item['comments']})
        

 
def get_like(message_id):
    #print message_id
    likes_count = urllib2.Request("https://graph.facebook.com/v2.5/"+ str(message_id) +"/likes?summary=1&filter=toplevel&access_token="+ACCESS_TOKEN)
    counts = loads(urlopen(likes_count).read())
    likes_total_count = counts['summary']['total_count']
    return likes_total_count

def get_likeuser(message_id):
    likes_total_count = get_like(message_id)
    user = []
    likes_user = urllib2.Request("https://graph.facebook.com/v2.3/"+ str(message_id) +"/likes?limit="+str(likes_total_count)+"&access_token="+ACCESS_TOKEN)
    counts = loads(urlopen(likes_user).read())
    user_ids = counts['data']
    
    for li in user_ids:
        user_id = li['id']
        try:
            user_name = get_name(user_id)
        except:
            user_name = 'unknown'
        user_item = {
            'user_id':user_id,
            'user_name':user_name
        }
        user.append(user_item)
    try:
        next_page = counts['paging']['next']
        more_user = next(next_page)
        for i in range(0,len(more_user)):
            user.append(more_user[i])
    except:
        print "aa"
    return user

def get_comment(message_id):
    comments = []
    comments_count = urllib2.Request("https://graph.facebook.com/v2.5/"+ str(message_id) +"/comments?summary=1&filter=toplevel&access_token="+ACCESS_TOKEN)
    counts = loads(urlopen(comments_count).read())
    comments_total_count = counts['summary']['total_count']
    return comments_total_count

def get_commentuser(message_id):
    comments_total_count = get_comment(message_id)
    comment_info = []
    comment_user = urllib2.Request("https://graph.facebook.com/v2.3/"+ str(message_id) +"/comments?limit="+str(comments_total_count)+"&access_token="+ACCESS_TOKEN)
    counts = loads(urlopen(comment_user).read())
    comments = counts['data']
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
        next_page = counts['paging']['next']
        more_user = nextComment(next_page)
        for i in range(0,len(more_user)):
            user.append(more_user[i])
    except Exception,e:
        print e
    return comment_info


def get_name(user_id):
    reqs = urllib2.Request("https://graph.facebook.com/v2.5/"+ str(user_id)+"?access_token="+ACCESS_TOKEN)
    con = loads(urlopen(reqs).read())
    if isinstance(con['name'],unicode):
        name = con['name'].encode('utf-8')
    else:
        name = con['name']
    return name

def next(next_page):
    user = []
    reqs = urllib2.Request(next_page)
    con = loads(urlopen(reqs).read())
    user_ids = con['data']
    
    for li in user_ids:
        user_id = li['id']
        try:
            user_name = get_name(user_id)
        except:
            user_name = 'unknown'
        user_item = {
            'user_id':user_id,
            'user_name':user_name
        }
        user.append(user_item)
    try:
        next_page = con['paging']['next']
        more_user = next(next_page)
    except:
        print "aaa"
    return user

def nextComment(next_page,message_id,comment_count):

    comment_info = []
    reqs = urllib2.Request(next_page)
    con = loads(urlopen(reqs).read())
    comments = con['data']
    for li in comments:
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
    item = {
        'm_id':message_id,
        'comment_count':comment_count,
        'author':'soong',
        'comments':comment_info
    }
    process_item(item)
    try:
        next_page = con['paging']['next']
        more_user = nextComment(next_page,message_id,comment_count)
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
    f = open('soong08.csv', 'rb')
    reader = csv.reader(f)
    for li in reader:
        print li[0]
        main(li[0])
