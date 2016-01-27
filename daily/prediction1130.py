# -*- coding: cp936 -*-
# -*- coding：utf-8 -*-
#from langconv import *
import os
import csv
import types
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import time
import datetime
import pymongo
from pymongo import MongoClient
import json
import logging 

MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
conn = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
MONGODB_DB = 'taiwan'
MONGOD_COLLECTION_1 = 'fb_status_tsaiingwen'
MONGOD_COLLECTION_2 = 'fb_status_llchu'
MONGOD_COLLECTION_3 = 'fb_status_soong'
MONGOD_COLLECTION_4 = 'fb_status_chuchupepper'
MONGOD_COLLECTION_5 = 'tisr'
MONGOD_COLLECTION_6 = 'prediction_result'

RATIO_20_40 = 0.3796
RATIO_40 = 0.1930
RATIO_50 = 0.1920
RATIO_60 = 0.1313
RATIO_70 = 0.1042
POLL_WEIGHT = 0
FB_WEIGHT = 0

mongodb = conn[MONGODB_DB]
collection1 = mongodb['days']
DAYS = collection1.find().sort('timestamp',pymongo.DESCENDING)
NUM = []
for li in DAYS:
    NUM.append(li['prediction_day'])
CAL_DAY = NUM[0]


def ts2HMS(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

def HMS2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d')))

def HMS2ts_1(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d %H:%M:%S')))

def ts2HMS_1(ts):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(ts))

def poll_data():
    '''
    读入民调数据
    '''
    date =  ['2015-10-15','2015-10-29','2015-11-12','2015-11-27']
    poll_dict = {}
    with open('./age_vote.csv','rb')as f:
        reader = csv.reader(f)
        i = 0
        for row in reader:
            poll_dict[date[i]] = row
            i += 1

    return poll_dict

def read_poll(end,poll_dict):
    '''
    找到最近的民调数据
    输入数据：
    end:当前时间节点
    poll_dict:{"民调发布时间":[蔡支持率（40-49岁），蔡支持率（50-59岁），蔡支持率（60-69岁），蔡支持率（70岁以上）,...(其他二人)]}
    '''
    time_interval = []
    timestamp = []
    for k,v in poll_dict.iteritems():
        poll_time = HMS2ts(k)
        if end-poll_time >0:
            time_interval.append(end-HMS2ts(k))
            timestamp.append(k)
        else:
            time_interval.append(end)
            timestamp.append(k)
    #print timestamp[time_interval.index(min(time_interval))]

    return poll_dict[timestamp[time_interval.index(min(time_interval))]]


def read_poll_all():
    '''
    读入每半个月民调（综合结果），找到最近民调数据
    '''
    date = ['2015-10-15','2015-10-29','2015-11-12','2015-11-27','2015-12-14']
    poll_all_dict = {}
    with open('./vote_all_age.csv','rb') as f:
        reader = csv.reader(f)
        i = 0
        for row in reader:
            poll_all_dict[date[i]] = row
            i+= 1

    return poll_all_dict

def poll_mongo():
    mongodb = conn[MONGODB_DB]
    collection = mongodb[MONGOD_COLLECTION_5]
    results = collection.find()
    poll_data = {}#民调数据，{"日期"：{"人":[总支持率，20岁支持率，30岁支持率，...,70岁以上支持率]}}
    for li in results:
        print li
        if poll_data.has_key(li["date"]):
            poll_data[li["date"]][li["person"]] = [li["relative_support"],li["20s_relative"],li["30s_relative"],li["40s_relative"],li["50s_relative"],li["60s_relative"],li["70s_relative"]]
        else:
            poll_data[li["date"]] = {}
            poll_data[li["date"]][li["person"]] = [li["relative_support"],li["20s_relative"],li["30s_relative"],li["40s_relative"],li["50s_relative"],li["60s_relative"],li["70s_relative"]]
    return poll_data    

def poll_mongo_relative(end):
    '''
    从mongo中读入民调数据,将民调数据归一化
    '''
    mongodb = conn[MONGODB_DB]
    collection = mongodb[MONGOD_COLLECTION_5]
    results = collection.find()
    poll_data = {}#民调数据，{"日期"：{"人":[总支持率，20岁支持率，30岁支持率，...,70岁以上支持率]}}
    for li in results:#将没有归一化的民调读出
        try:
            poll_data[li["date"]][li["person"]] = [li["_id"],li["support_rate"],li["20s"],li["30s"],li["40s"],li["50s"],li["60s"],li["70s"]]
        except KeyError:
            poll_data[li["date"]] = {}
            poll_data[li["date"]][li["person"]] = [li["_id"],li["support_rate"],li["20s"],li["30s"],li["40s"],li["50s"],li["60s"],li["70s"]]

    relative_support = {}
    for k,v in poll_data.iteritems():#遍历每期民调
        relative_support[k] = {}
        total_support = 0
        total_20 = 0
        total_30 = 0
        total_40 = 0
        total_50 = 0
        total_60 = 0
        total_70 = 0
        for key, value in v.iteritems():#计算三位参选人总支持率、各年龄段支持率之和
            total_support  += float(value[1])#三位参选人总支持率之和
            total_20 += float(value[2])#三位参选人20岁总支持率之和
            total_30 += float(value[3])#三位参选人30岁总支持率之和
            total_40 += float(value[4])#三位参选人40岁总支持率之和
            total_50 += float(value[5])#三位参选人50岁总支持率之和
            total_60 += float(value[6])#三位参选人60岁总支持率之和
            total_70 += float(value[7])#三位参选人70岁总支持率之和

        item = {}
        for key_1,value_1 in v.iteritems():#民调归一化
            _id = value_1[0]
            support_total = float(value_1[1])/total_support
            support_20 = float(value_1[2])/total_20
            support_30 = float(value_1[3])/total_30
            support_40 = float(value_1[4])/total_40
            support_50 = float(value_1[5])/total_50
            support_60 = float(value_1[6])/total_60
            support_70 = float(value_1[7])/total_70
            relative_support[k][key_1] = [support_total, support_20, support_30,support_40,support_50,support_60,support_70]
            update_modifier = {'$set':{"relative_support":support_total,"20s_relative":support_20,"30s_relative":support_30,"40s_relative":support_40,"50s_relative":support_50,"60s_relative":support_60,"70s_relative":support_70}}
            collection.update({"_id":_id},update_modifier)

    #读入最近一期归一化后的民调数据
    time_interval = []
    timestamp = []
    for k,v in relative_support.iteritems():
        if end - HMS2ts(k)>=0:
            time_interval.append(end-HMS2ts(k))
            timestamp.append(k)
        else:
            time_interval.append(end)
            timestamp.append(k)
    
    latest_time = timestamp[time_interval.index(min(time_interval))]

    return poll_data[latest_time]


def read_poll_mongo(end, poll_data):
    '''
    从mongo数据库中读出的民调数据中查找最近数据
    输入数据：
    end：当前时间节点
    poll_data:{"日期"：{"人":[总支持率，20岁支持率，30岁支持率，...,70岁以上支持率]}}
    输出数据：
    poll_data[latest_time]:最近一起民调的数据，{"人":[总支持率，20岁支持率，30岁支持率，...,70岁以上支持率]}
    '''
    time_interval = []
    timestamp = []
    for k,v in poll_data.iteritems():
        if end - HMS2ts(k)>0:
            time_interval.append(end-HMS2ts(k))
            timestamp.append(k)
        else:
            time_interval.append(end)
            timestamp.append(k)

    latest_time = timestamp[time_interval.index(min(time_interval))]

    return poll_data[latest_time]
                
def interval_average_favorite_v1(end,COLLECTION_NAME,author):
    mongodb = conn[MONGODB_DB]
    collection = mongodb[COLLECTION_NAME]
    start = end-CAL_DAY*24*60*60
    results = collection.find({"author":author})
    #print author,results.count()
    daily_count_dict = {}#{"_id":{"日期"：赞数，“日期”：赞数,....}}
    zan_count = {}#{"_id":赞数}
    interval_average = {}
    date = {}
    not_in_time_count = 0
    for li in results:
        if float(li["created_time_timestamp"])>=start and float(li["created_time_timestamp"])<=end:
            #daily_count_dict[li["_id"]] = li["daily_count"]
            if li.has_key("duplicate") ==True and li["duplicate"]== 'true':
                continue
            else:
                zan_count[li["_id"]] = li["per_favourity_count"]
                if type(li["daily_count"]) != dict:
                    for item in li["daily_count"].split('{')[1].split(','):
                        if "\'" in item.split(':')[0]:
                            try:
                                if '}' in item.split(':')[-1].split("\'")[1]:
                                    daily_count_dict[li["_id"]][str(item.split(':')[0].split('\'')[1].split(' ')[0])] = int(item.split(':')[-1].split("\'")[1].split('}')[0])
                                else:
                                    daily_count_dict[li["_id"]][str(item.split(':')[0].split('\'')[1].split(' ')[0])] = int(item.split(':')[-1].split("\'")[1])
                            except KeyError:
                                daily_count_dict[li["_id"]] = {}
                                if '}' in item.split(':')[-1].split("\'")[1]:
                                    daily_count_dict[li["_id"]][str(item.split(':')[0].split('\'')[1].split(' ')[0])] = int(item.split(':')[-1].split("\'")[1].split('}')[0])
                                else:
                                    daily_count_dict[li["_id"]][str(item.split(':')[0].split('\'')[1].split(' ')[0])] = int(item.split(':')[-1].split("\'")[1])
                        elif "\"" in item.split(':')[-1]:
                            try:
                                if '}' in item.split(':')[-1].split('\"')[1]:
                                    daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\"')[1].split('}')[0])
                                else:
                                    daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\"')[1])
                            except KeyError:
                                daily_count_dict[li["_id"]] = {}
                                if '}' in item.split(':')[-1].split('\"')[1]:
                                    daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\"')[1].split('}')[0])
                                else:
                                    daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\"')[1])
                        else:
                            if "\'" in item.split(':')[-1]:
                                try:
                                    if '}' in item.split(':')[-1].split('\'')[1]:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\'')[1].split('}')[0])
                                    else:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\'')[1])
                                except KeyError:
                                    daily_count_dict[li["_id"]] = {}
                                    if '}' in item.split(':')[-1].split('\'')[1]:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\'')[1].split('}')[0])
                                    else:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\'')[1])
                            elif "\"" in item.split(':')[-1]:
                                try:
                                    if '}' in item.split(':')[-1].split('\"')[1]:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\"')[1].split('}')[0])
                                    else:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\"')[1])
                                except KeyError:
                                    daily_count_dict[li["_id"]] = {}
                                    if '}' in item.split(':')[-1].split('\"')[1]:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\"')[1].split('}')[0])
                                    else:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\"')[1])
                            else:
                                try:
                                    if '}' in item.split(':')[-1]:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('}')[0])
                                    else:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1])
                                except KeyError:
                                    daily_count_dict[li["_id"]] = {}
                                    if '}' in item.split(':')[-1]:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('}')[0])
                                    else:
                                        daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1])
                else:
                    daily_count_dict[li["_id"]] = li["daily_count"]
                try:
                    date[ts2HMS(float(li["created_time_timestamp"]))][li["_id"]]=daily_count_dict[li["_id"]]
                except KeyError:
                    date[ts2HMS(float(li["created_time_timestamp"]))]={}
                    date[ts2HMS(float(li["created_time_timestamp"]))][li["_id"]]=daily_count_dict[li["_id"]]
            
        else:
            not_in_time_count += 1
        

    favourite_count = {}#{"_id":最接近统计时间节点的赞数}
    date_favourite_count = {}#{"date":{"_id":{"时间"：赞数}}}
    for k,v in daily_count_dict.iteritems():
        everyday_count = {}#每条帖子每天的赞数
##        print k,v,type(v)
        for key, value in v.iteritems():
            if ':' in key:
                time_key = HMS2ts_1(key)
            else:
                time_key = HMS2ts(key.strip("\"")[0:10])
            if (time_key >=start) and (time_key<=end):
                everyday_count[time_key]=value
        #找到时间区间内最接近统计时间节点的记录
        if len(everyday_count) == 0:
            zan = zan_count[k]
        else:
            zan = everyday_count[max(everyday_count.keys())]
        favourite_count[k] = float(zan)

    for k,v in date.iteritems():
        for key,value in v.iteritems():
            try:
                date_favourite_count[k][key]=favourite_count[key]
            except KeyError:
                date_favourite_count[k] = {}
                date_favourite_count[k][key]=favourite_count[key]

    #平均每天帖子赞数
    average_date_favourite_count = {}
    for k,v in date_favourite_count.iteritems():
        daily_count = []
        for key,value in v.iteritems():
            daily_count.append(value)
        average = float(sum(daily_count))/float(len(daily_count))
        average_date_favourite_count[k] = [average,sum(daily_count),len(daily_count)]

    #30天内平均
    count = 0
    for k,v in average_date_favourite_count.iteritems():
        count += v[0]
    average = float(count)/float(len(average_date_favourite_count))
    interval_average[start] = average
    
    return interval_average
            

def interval_average_favorite(end,COLLECTION_NAME,author):
    '''
    计算给定时间前30天以内候选人帖子赞数
    输入数据：
    end:当前时间节点
    COLLECTION_NAME:数据库名称
    author：collection名称
    输出数据：
    week_average:{"开始时间":平均赞数}
    favourite_count:{"_id":赞数}
    '''
    mongodb = conn[MONGODB_DB]
    collection = mongodb[COLLECTION_NAME]
    start = end-CAL_DAY*24*60*60#计算开始时间
    week_average = {}
    results = collection.find({"author":author,"created_time_timestamp":{"$gte" : start, "$lte" : end}})#获取最近30天fb数据
    daily_count_dict = {}#{"_id":{"日期"：赞数，“日期”：赞数,....}}
    zan_count = {}#{"_id":赞数}
    for li in results:
        daily_count_dict[li["_id"]] = li["daily_count"]#读取每条帖子历史赞数数据，{“日期”：当日赞数}
        zan_count[li["_id"]] = li["per_favourity_count"]#读取每条帖子每日赞数数据
        if type(li["daily_count"]) != dict:#如果每条帖子赞数的历史数据存储格式不为字典，需对数据进行处理，转换成字典格式
            #daily_count_dict[li["_id"]] = {li["daily_count"].split('{')[1].split(':')[0]:int(li["daily_count"].split('}')[0].split(':')[1].strip("\""))}
            for item in li["daily_count"].split('{')[1].split(','):
                if "\'" in item.split(':')[0]:
                    try:
                        if '}' in item.split(':')[-1].split("\'")[1]:
                            daily_count_dict[li["_id"]][str(item.split(':')[0].split('\'')[1].split(' ')[0])] = int(item.split(':')[-1].split("\'")[1].split('}')[0])
                        else:
                            daily_count_dict[li["_id"]][str(item.split(':')[0].split('\'')[1].split(' ')[0])] = int(item.split(':')[-1].split("\'")[1])
                    except KeyError:
                        daily_count_dict[li["_id"]] = {}
                        if '}' in item.split(':')[-1].split("\'")[1]:
                            daily_count_dict[li["_id"]][str(item.split(':')[0].split('\'')[1].split(' ')[0])] = int(item.split(':')[-1].split("\'")[1].split('}')[0])
                        else:
                            daily_count_dict[li["_id"]][str(item.split(':')[0].split('\'')[1].split(' ')[0])] = int(item.split(':')[-1].split("\'")[1])
                elif "\"" in item.split(':')[-1]:
                    try:
                        if '}' in item.split(':')[-1].split('\"')[1]:
                            daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\"')[1].split('}')[0])
                        else:
                            daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\"')[1])
                    except KeyError:
                        daily_count_dict[li["_id"]] = {}
                        if '}' in item.split(':')[-1].split('\"')[1]:
                            daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\"')[1].split('}')[0])
                        else:
                            daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\"')[1])
                else:
                    if "\'" in item.split(':')[-1]:
                        try:
                            if '}' in item.split(':')[-1].split('\'')[1]:
                                daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\'')[1].split('}')[0])
                            else:
                                daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\'')[1])
                        except KeyError:
                            daily_count_dict[li["_id"]] = {}
                            if '}' in item.split(':')[-1].split('\'')[1]:
                                daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\'')[1].split('}')[0])
                            else:
                                daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\'')[1])
                    elif "\"" in item.split(':')[-1]:
                        try:
                            if '}' in item.split(':')[-1].split('\"')[1]:
                                daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\"')[1].split('}')[0])
                            else:
                                daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\"')[1])
                        except KeyError:
                            daily_count_dict[li["_id"]] = {}
                            if '}' in item.split(':')[-1].split('\"')[1]:
                                daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\"')[1].split('}')[0])
                            else:
                                daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('\"')[1])
                    else:
                        try:
                            if '}' in item.split(':')[-1]:
                                daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('}')[0])
                            else:
                                daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1])
                        except KeyError:
                            daily_count_dict[li["_id"]] = {}
                            if '}' in item.split(':')[-1]:
                                daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1].split('}')[0])
                            else:
                                daily_count_dict[li["_id"]][item.split(':')[0]] = int(item.split(':')[-1])
        else:#如果每帖每日赞数数据为字典格式
            daily_count_dict[li["_id"]] = li["daily_count"]
##        try:#将一天内全部帖子及赞数数据汇总，{“日期”：{“帖子id”：{“日期”：赞数}}}
##            date[ts2HMS(float(li["created_time_timestamp"]))][li["_id"]]=daily_count_dict[li["_id"]]
##        except KeyError:
##            date[ts2HMS(float(li["created_time_timestamp"]))]={}
##            date[ts2HMS(float(li["created_time_timestamp"]))][li["_id"]]=daily_count_dict[li["_id"]]

    favourite_count = {}#{"_id":最接近统计时间节点的赞数}
    for k,v in daily_count_dict.iteritems():
        everyday_count = {}#每条帖子每天的赞数
        for key, value in v.iteritems():
            if ':' in key:
                time_key = HMS2ts_1(key)
            else:
                time_key = HMS2ts(key.strip("\"")[0:10])
            if (time_key >=start) and (time_key<=end):
                everyday_count[time_key]=value
        #找到时间区间内最接近统计时间节点的记录
        if len(everyday_count) == 0:
            zan = zan_count[k]
        else:
            zan = everyday_count[max(everyday_count.keys())]
        favourite_count[k] = float(zan)

    #计算30天平均赞数
    if len(favourite_count) == 0:
        week_average[start] = 0
        #print ts2HMS(start)
    else:
        average = float(sum(favourite_count.values()))/float(len(favourite_count))
        week_average[start] = average

    return week_average

def interval_vote_rate(end,p1,p2,p3):
    '''
    计算三位参选人一段时间的赞数之比
    输入数据：
    interval:[[开始时间，结束时间],[开始时间，结束时间]]（以秒为单位）
    p1_week:{"开始时间":赞数}
    '''
    p1_average = p1[end-CAL_DAY*24*60*60]
    p2_average = p2[end-CAL_DAY*24*60*60]
    p3_average = p3[end-CAL_DAY*24*60*60]
    sum_average = p1_average + p2_average + p3_average
    p1_vote = float(p1_average)/sum_average
    p2_vote = float(p2_average)/sum_average
    p3_vote = float(p3_average)/sum_average
    vote = [p1_vote,p2_vote,p3_vote]
   
    return vote

def interval_statistic(end):
    '''
    一段时间内候选人支持率
    输入数据：
    end:当前时间节点
    输出数据：
    vote_rate:{"计算开始时间":[蔡支持率，朱支持率，宋支持率]}
    '''
    tsai = interval_average_favorite_v1(end,MONGOD_COLLECTION_1,"tsaiingwen")
    llchu = interval_average_favorite_v1(end,MONGOD_COLLECTION_2,"llchu")
    soong = interval_average_favorite_v1(end,MONGOD_COLLECTION_3,"soong")
    vote_rate = interval_vote_rate(end, tsai, llchu, soong)

    return vote_rate

def combine_vote(cal_end,fb,poll):
    '''
    系统自动权重：facebook和民调数据融合支持率
    '''

    #poll40岁以上人的支持率，按人口比例加权
    tsai_poll_vote = RATIO_40*float(poll["tsaiingwen"][3])+RATIO_50*float(poll["tsaiingwen"][4])+RATIO_60*float(poll["tsaiingwen"][5])+RATIO_70*float(poll["tsaiingwen"][6])
    llchu_poll_vote = RATIO_40*float(poll["llchu"][3])+RATIO_50*float(poll["llchu"][4])+RATIO_60*float(poll["llchu"][5])+RATIO_70*float(poll["llchu"][6])
    soong_poll_vote = RATIO_40*float(poll["soong"][3])+RATIO_50*float(poll["llchu"][4])+RATIO_60*float(poll["soong"][5])+RATIO_70*float(poll["soong"][6])

    #fb20-40岁支持率，按人口比例加权
    tsai_fb_vote = RATIO_20_40*float(fb[0])
    llchu_fb_vote = RATIO_20_40*float(fb[1])
    soong_fb_vote = RATIO_20_40*float(fb[2])

    #总支持率
    tsai_vote = tsai_poll_vote + tsai_fb_vote
    llchu_vote = llchu_poll_vote + llchu_fb_vote
    soong_vote = soong_poll_vote + soong_fb_vote

    #总支持率归一化
    tsai_final_vote = tsai_vote/(tsai_vote+llchu_vote+soong_vote)
    llchu_final_vote = llchu_vote/(tsai_vote+llchu_vote+soong_vote)
    soong_final_vote = soong_vote/(tsai_vote+llchu_vote+soong_vote)

    return tsai_final_vote,llchu_final_vote, soong_final_vote, poll["tsaiingwen"][1],poll["llchu"][1],poll["soong"][1],fb[0],fb[1], fb[2]

def combine_vote_expert(end,fb,poll,POLL_WEIGHT,FB_WEIGHT):
    '''
    专家权重：facebook和民调数据融合支持率
    '''
    tsai_vote = float(fb[0])*FB_WEIGHT + float(poll["tsaiingwen"][0])*POLL_WEIGHT
    llchu_vote = float(fb[1])*FB_WEIGHT +float(poll["llchu"][0])*POLL_WEIGHT
    soong_vote = float(fb[2])*FB_WEIGHT+ float(poll["soong"][0])*POLL_WEIGHT

    tsai_final_vote = tsai_vote/(tsai_vote+llchu_vote+soong_vote)
    llchu_final_vote = llchu_vote/(tsai_vote+llchu_vote+soong_vote)
    soong_final_vote = soong_vote/(tsai_vote+llchu_vote+soong_vote)

    return tsai_final_vote,llchu_final_vote, soong_final_vote, poll["tsaiingwen"][0],poll["llchu"][0],poll["soong"][0],fb[0],fb[1], fb[2]

def main(POLL_WEIGHT,FB_WEIGHT):
    '''
    主函数：计算每日的支持率预测值，每天24点启动。
    输入数据：
    poll_weight:民调在预测中的权重
    fb_weight:facebook在预测中的权重
    系统默认当上述两个权重均为0时，自动给出权重
    '''
    today = HMS2ts(datetime.date.today().strftime("%Y-%m-%d"))#获取当前时间节点
    fb = interval_statistic(today)#计算fb30天内支持率之比
    poll = poll_mongo_relative(today)#民调数据归一化,找出距离当前时间节点最近的一期民调
    #poll = read_poll_mongo(today,poll_dict)
    if POLL_WEIGHT ==0 and FB_WEIGHT == 0:#系统默认民调、fb权重
        tsai,llchu,soong,tsai_poll,llchu_poll,soong_poll,tsai_fb,llchu_fb,soong_fb = combine_vote(today, fb, poll)
    else:#专家给出权重
        if POLL_WEIGHT + FB_WEIGHT != 1:#如果民调权重和fb权重相加不为1，报错
            print ' Weight Error: The sum of two weights is not 1!'
        else:#民调权重和fb权重相加为1，按权重计算
            tsai,llchu,soong,tsai_poll,llchu_poll,soong_poll,tsai_fb,llchu_fb,soong_fb = combine_vote_expert(today,fb,poll,POLL_WEIGHT,FB_WEIGHT)

    #计算结果更新数据库
    mongodb = conn[MONGODB_DB]
    collection = mongodb[MONGOD_COLLECTION_6]
    collection.insert({"date":today,"timestamp":ts2HMS(today),"tsai":tsai,"llchu":llchu,"soong":soong,"tsai_poll":tsai_poll,"llchu_poll":llchu_poll,"soong_poll":soong_poll,"tsai_fb":tsai_fb,"llchu_fb":llchu_fb,"soong_fb":soong_fb})

    #return tsai,llchu,soong

if __name__ == '__main__':
    
    #将民调数据转换为相对量
    #poll_mongo_relative()

    #实验
    logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='prediction.log',
                filemode='a')
    today = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=1)
    yesterday = today - datetime.timedelta(days=1)
    end = str(yesterday)
    now = str(tomorrow)
    #end = '2015-10-30'
    #now = '2016-01-14'
    day = int(HMS2ts(now)-HMS2ts(end))/(24*60*60)
##    poll_dict = poll_mongo_relative()
##    poll_dict = poll_mongo()
    vote_result = {}
    mongodb = conn[MONGODB_DB]
    collection = mongodb[MONGOD_COLLECTION_6]
    start_time = time.time()
    logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='prediction.log',
                filemode='a')
    logging.info(' start prediction:' + str(ts2HMS_1(time.time())) )
    for i in range(day):
        cal_day = HMS2ts(end) + i*24*60*60
        print ts2HMS(cal_day)
        fb = interval_statistic(cal_day)
        #poll = read_poll_mongo(cal_day,poll_dict)
        poll = poll_mongo_relative(cal_day)
        tsai,llchu,soong,tsai_poll,llchu_poll,soong_poll,tsai_fb,llchu_fb,soong_fb = combine_vote(cal_day,fb,poll)
        vote_result[cal_day] = [tsai,llchu,soong,tsai_poll,llchu_poll,soong_poll,tsai_fb,llchu_fb,soong_fb]
        #collection.insert({"date":cal_day,"timestamp":ts2HMS(cal_day),"tsai":tsai,"llchu":llchu,"soong":soong,"tsai_poll":tsai_poll,"llchu_poll":llchu_poll,"soong_poll":soong_poll,"tsai_fb":tsai_fb,"llchu_fb":llchu_fb,"soong_fb":soong_fb})
        if collection.find({"date":cal_day}).count():
            collection.update({"date":cal_day},{"$set":{"timestamp":ts2HMS(cal_day),"tsai":tsai,"llchu":llchu,"soong":soong,"tsai_poll":tsai_poll,"llchu_poll":llchu_poll,"soong_poll":soong_poll,"tsai_fb":tsai_fb,"llchu_fb":llchu_fb,"soong_fb":soong_fb}})
        else:
            collection.insert({"date":cal_day,"timestamp":ts2HMS(cal_day),"tsai":tsai,"llchu":llchu,"soong":soong,"tsai_poll":tsai_poll,"llchu_poll":llchu_poll,"soong_poll":soong_poll,"tsai_fb":tsai_fb,"llchu_fb":llchu_fb,"soong_fb":soong_fb})
##
    end_time = time.time()
    logging.info(' end prediction:' + str(ts2HMS_1(time.time())) )
    cal_time = float(end_time)-float(start_time)
    logging.info('total cal_time prediction :'+ str(cal_time)  )
    print "cal_time:"+str(cal_time)
    

##    #测试
##    POLL_WEIGHT = 0.5
##    FB_WEIGHT = 0.5
##    tsai,llchu,soong = main(POLL_WEIGHT,FB_WEIGHT)
##    print tsai, llchu, soong
##        
    
