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
MONGOD_COLLECTION = 'llchu'
mongodb = conn[MONGODB_DB]
collection = mongodb[MONGOD_COLLECTION]
ACCESS_TOKEN = ''
localtime =  time.strftime("%Y-%m-%d ",time.localtime(time.time()))

def main():
    req = urllib2.Request('https://graph.facebook.com/v2.5/llchu/?fields=id,posts,likes&access_token='+ACCESS_TOKEN)
    content = loads(urlopen(req).read())
    user_id = content['id']
    likes = content['likes']
    print user_id,likes
    end = int(time.time())
    print end
    post = get_post(user_id,end)
    '''
    f = open('fb_tsaiingwen.csv','wb')
    writer = csv.writer(f)
    writer.writerow(['m_id','message','time','timestamp','likes_user','comment_user'])
    flist = ['m_id','message','time','timestamp','likes_user','comment_user']
    
    f = open('fb_tsaiingwen.jl','ab')
    for li in post:
        lines = json.dumps(li)
        f.write(lines+'\n')
    f.close()
    '''
def get_post(user_id,end):
    user_id = user_id
    timestore = []
    msm = []
    req_post = urllib2.Request("https://graph.facebook.com/v2.5/"+str(user_id)+"/posts?limit=100&since=2015-06-01&until="+ str(end)+"&access_token="+ACCESS_TOKEN)
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
        #comment_user = get_commentuser(message_id)
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
            'author':'llchu'
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
        collection.update({'m_id':item['m_id']},updates_modifier)
    else:
        collection.insert({'m_id':item['m_id'],'content':item['content'],'time':item['time'],'created_time_timestamp':item['created_time_timestamp'],'author':item['author'],\
                            'per_favourity_count':item['per_favourity_count'],'comment_count':item['comment_count'],'daily_count':{localtime:item['per_favourity_count']}})
        

 
def get_like(message_id):
    #print message_id
    user = []
    likes_count = urllib2.Request("https://graph.facebook.com/v2.5/"+ str(message_id) +"/likes?summary=1&filter=toplevel&access_token="+ACCESS_TOKEN)
    counts = loads(urlopen(likes_count).read())
    likes_total_count = counts['summary']['total_count']
    #print likes_total_count
    
    return likes_total_count

def get_comment(message_id):
    
    comments = []
    comments_count = urllib2.Request("https://graph.facebook.com/v2.5/"+ str(message_id) +"/comments?summary=1&filter=toplevel&access_token="+ACCESS_TOKEN)
    counts = loads(urlopen(comments_count).read())
    comments_total_count = counts['summary']['total_count']
    #print comments_total_count
    
    return comments_total_count

def get_name(user_id):
    reqs = urllib2.Request("https://graph.facebook.com/v2.5/"+ str(user_id)+"?access_token="+ACCESS_TOKEN)
    con = loads(urlopen(reqs).read())
    if isinstance(con['name'],unicode):
        name = con['name'].encode('utf-8')
        
    else:
        name = con['name']
    
    return name

def time2stamp(timestr):
    yyddmm = timestr.split('T')[0]
    hhmmss = timestr.split('T')[1].split('+')[0]
    datestr = yyddmm + ' '+hhmmss
    timestamp = int(time.mktime(time.strptime(datestr,'%Y-%m-%d %H:%M:%S')))
    return timestamp

if __name__ == '__main__':
    main()
