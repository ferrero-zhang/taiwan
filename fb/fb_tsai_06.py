# -*- coding: utf-8 -*-

import urllib2
from urllib2 import urlopen
from simplejson import loads
import time
import csv
import json
import pymongo
from pymongo import MongoClient
from config import(
    MONGODB_HOST,
    MONGODB_PORT,
    MONGODB_DB,
    MONGOD_COLLECTION,
    ACCESS_TOKEN,
    )
conn = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)

mongodb = conn[MONGODB_DB]
collection = mongodb[MONGOD_COLLECTION]

localtime =  time.strftime("%Y-%m-%d ",time.localtime(time.time()))

def main():
    req = urllib2.Request('https://graph.facebook.com/v2.5/tsaiingwen/?fields=id,posts,likes&access_token='+ACCESS_TOKEN)
    content = loads(urlopen(req).read())
    user_id = content['id']
    likes = content['likes']
    print user_id,likes
    end = int(time.time())
    print end
    post = get_post(user_id,end)

def get_post(user_id,end):
    user_id = user_id
    timestore = []
    msm = []
    req_post = urllib2.Request("https://graph.facebook.com/v2.5/"+str(user_id)+"/posts?limit=100&since=2015-06-01&until=2015-07-01&access_token="+ACCESS_TOKEN)
    #print req_post
    try:
        con = loads(urlopen(req_post).read())
        posts = con['data']
        for li in posts:
            message_id = li['id']
            print message_id
            try:
                messages = li['message']
            except:
                messages = ''
            message_created = li['created_time']
            timestamp = time2stamp(message_created)
            likes_count = get_like(message_id)
            comment_count = get_comment(message_id)
            like_users = get_likeuser(message_id)
            #comment_users = get_commentuser(message_id)
         
            item = {
                'm_id':message_id,
                'content':messages,
                'time':message_created,
                'created_time_timestamp':timestamp,
                'per_favourity_count':likes_count,
                'comment_count':comment_count,
                'author':'tsai',
                'like_user':like_users
                #'comment_user':comment_users
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
    except urllib2.URLError,e:
        if hasattr(e,"reason"):
            print "Failed to reach the server"
            print "The reason:",e.reason
        elif hasattr(e,"code"):
            print "The server couldn't fulfill the request"
            print "Error code:",e.code
            print "Return content:",e.read()
    return msm

def process_item(item):
    if collection.find({'m_id':item['m_id']}).count():
        findli = collection.find({'m_id':item['m_id']})
        findli = findli[0]
        try:
            if findli['daily_count']:
                findli['daily_count'].update({localtime:item['per_favourity_count']})
            else:
                findli['daily_count'] = {localtime:item['per_favourity_count']}
        except:
            findli['daily_count'] = {localtime:item['per_favourity_count']}
        updates_modifier = {'$set':findli}
        collection.update({'m_id':item['m_id']},{'$set':item})
        collection.update({'m_id':item['m_id']},updates_modifier)
        
    else:
        collection.insert({'m_id':item['m_id'],'content':item['content'],'time':item['time'],'created_time_timestamp':item['created_time_timestamp'],'author':item['author'],\
                            'per_favourity_count':item['per_favourity_count'],'comment_count':item['comment_count'],'daily_count':{localtime:item['per_favourity_count']},\
                            'like_user':item['like_user']})
        

 
def get_like(message_id):
    #print message_id
    likes_count = urllib2.Request("https://graph.facebook.com/v2.5/"+ str(message_id) +"/likes?summary=1&filter=toplevel&access_token="+ACCESS_TOKEN)
    try:
        counts = loads(urlopen(likes_count).read())
        likes_total_count = counts['summary']['total_count']
    except urllib2.URLError,e:
        if hasattr(e,"reason"):
            print "Failed to reach the server"
            print "The reason:",e.reason
        elif hasattr(e,"code"):
            print "The server couldn't fulfill the request"
            print "Error code:",e.code
            print "Return content:",e.read()
    return likes_total_count

def get_likeuser(message_id):
    likes_total_count = get_like(message_id)
    user = []
    likes_user = urllib2.Request("https://graph.facebook.com/v2.3/"+ str(message_id) +"/likes?limit="+str(likes_total_count)+"&access_token="+ACCESS_TOKEN)
    try:
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
    except urllib2.URLError,e:
        if hasattr(e,"reason"):
            print "Failed to reach the server"
            print "The reason:",e.reason
        elif hasattr(e,"code"):
            print "The server couldn't fulfill the request"
            print "Error code:",e.code
            print "Return content:",e.read()
    return user

def get_comment(message_id):
    comments = []
    comments_count = urllib2.Request("https://graph.facebook.com/v2.5/"+ str(message_id) +"/comments?summary=1&filter=toplevel&access_token="+ACCESS_TOKEN)
    try:
        counts = loads(urlopen(comments_count).read())
        comments_total_count = counts['summary']['total_count']
    except urllib2.URLError,e:
        if hasattr(e,"reason"):
            print "Failed to reach the server"
            print "The reason:",e.reason
        elif hasattr(e,"code"):
            print "The server couldn't fulfill the request"
            print "Error code:",e.code
            print "Return content:",e.read()
    return comments_total_count

def get_commentuser(message_id):
    comments_total_count = get_comment(message_id)
    comment_info = []
    comment_user = urllib2.Request("https://graph.facebook.com/v2.3/"+ str(message_id) +"/comments?limit="+str(comments_total_count)+"&access_token="+ACCESS_TOKEN)
    try:
        counts = loads(urlopen(comment_user).read())
        comments = counts['data']
        for li in comments:
        #print li
            try:
                users = li['from']
                user_id = users['id']
                user_name = users['name']
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
    except urllib2.URLError,e:
        if hasattr(e,"reason"):
            print "Failed to reach the server"
            print "The reason:",e.reason
        elif hasattr(e,"code"):
            print "The server couldn't fulfill the request"
            print "Error code:",e.code
            print "Return content:",e.read()
    return comment_info


def get_name(user_id):
    reqs = urllib2.Request("https://graph.facebook.com/v2.5/"+ str(user_id)+"?access_token="+ACCESS_TOKEN)
    try:
        con = loads(urlopen(reqs).read())
        if isinstance(con['name'],unicode):
             name = con['name'].encode('utf-8')
        else:
            name = con['name']
    except urllib2.URLError,e:
        if hasattr(e,"reason"):
            print "Failed to reach the server"
            print "The reason:",e.reason
        elif hasattr(e,"code"):
            print "The server couldn't fulfill the request"
            print "Error code:",e.code
            print "Return content:",e.read()
    return name

def next(next_page):
    user = []
    reqs = urllib2.Request(next_page)
    try:
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
    except urllib2.URLError,e:
        if hasattr(e,"reason"):
            print "Failed to reach the server"
            print "The reason:",e.reason
        elif hasattr(e,"code"):
            print "The server couldn't fulfill the request"
            print "Error code:",e.code
            print "Return content:",e.read()
    return user


def time2stamp(timestr):
    yyddmm = timestr.split('T')[0]
    hhmmss = timestr.split('T')[1].split('+')[0]
    datestr = yyddmm + ' '+hhmmss
    timestamp = int(time.mktime(time.strptime(datestr,'%Y-%m-%d %H:%M:%S')))
    return timestamp

if __name__ == '__main__':
    main()
