#-*-coding=utf-8-*-

import time
import Levenshtein
import pymongo
from bson.objectid import ObjectId
import datetime
import logging

MONGOD_HOST = 'localhost'
MONGOD_PORT = 27017
MONGOD_DB = 'taiwan'
MONGOD_COLLECTION = 'news'

today = datetime.date.today()
tomorrow = today + datetime.timedelta(days=1)
yesterday = today - datetime.timedelta(days=1)
daysbefore = today - ((datetime.timedelta(days=1))*2)

def ts2HMS_1(ts):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(ts))

def _default_mongo(host=MONGOD_HOST, port=MONGOD_PORT, usedb=MONGOD_DB):
    # 强制写journal，并强制safe
    connection = pymongo.MongoClient(host=host, port=port, j=False, w=1)
    # db = connection.admin
    # db.authenticate('root', 'root')
    db = getattr(connection, usedb)
    return db

class ShingLing(object):
    """shingle算法
    """
    def __init__(self, text1, text2, n=3):
        """input
               text1: 输入文本1, unicode编码
               text2: 输入文本2, unicode编码
               n: 切片长度
        """
        if not isinstance(text1, unicode):
            raise ValueError("text1 must be unicode")

        if not isinstance(text2, unicode):
            raise ValueError("text2 must be unicode")

        self.n = n
        self.threshold = 0.3
        self.text1 = text1
        self.text2 = text2
        self.set1 = set()
        self.set2 = set()
        self._split(self.text1, self.set1)
        self._split(self.text2, self.set2)
        self.jaccard = 0

    def _split(self, text, s):
        if len(self.text1) < self.n:
            self.n = 1

        for i in range(len(text) - self.n + 1):
            piece = text[i: i + self.n]
            s.add(piece)

    def cal_jaccard(self):
        intersection_count = len(self.set1 & self.set2)
        union_count = len(self.set1 | self.set2)

        self.jaccard = float(intersection_count) / float(union_count)
        return self.jaccard

    def check_duplicate(self):
        return True if self.jaccard > self.threshold else False

def HMS2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d')))

def shingle_news(start_cal, end_cal):
    db = _default_mongo()
    #news1 =  db[MONGOD_COLLECTION].find({'timestamp':{'$gte':HMS2ts(str(yesterday),'$lte':)}})
    news1 =  db[MONGOD_COLLECTION].find({'timestamp':{'$gte':HMS2ts(start_cal),'$lte':HMS2ts(end_cal)}})
    news2 =  db[MONGOD_COLLECTION].find({'timestamp':{'$gte':HMS2ts(start_cal),'$lte':HMS2ts(end_cal)}})
    #print HMS2ts(start_cal),HMS2ts(end_cal),news1.count()
    summary1 = []
    ids1 = []
    times1 = []
    last1 = []
    keyword1 = []
    summary2 = []
    ids2 = []
    times2 = []
    last2 = []
    keyword2 = []
    for i in news1:
        summary1.append(i['summary'])
        ids1.append(i['_id'])
        times1.append(i['timestamp'])
        try:
            last1.append(i['last_moify'])
        except:
            last1.append(time.time())
        keyword1.append(i['keyword'])
    for j in news2:
        summary2.append(j['summary'])
        ids2.append(j['_id'])
        times2.append(j['timestamp'])
        try:
            last2.append(i['last_moify'])
        except:
            last2.append(time.time())
        keyword2.append(i['keyword'])
    count = 0
        
    for i in summary1:

        text1 = summary1[count]
        k = 0
        for j in summary2:
            text2 = summary2[k]

            s = ShingLing(text1,text2,3)
            #print s.cal_jaccard()
            if (s.check_duplicate()):
                if(ids1[count]!=ids2[k]):
                    if(float(times1[count])>float(times2[k])):
                        candidates = []
                        try:
                            candidate = db[MONGOD_COLLECTION].find({'_id':ObjectId(ids2[k])})[0]['candidate']
                            if isinstance(candidate,list):
                                candidates = candidate
                                if keyword1[count] not in candidate:
                                    candidates.append(keyword1[count])
                            elif(candidate == keyword1[count]):
                                candidates.append(keyword1[count])
                            else:
                                candidates.append(keyword1[count])
                                candidates.append(candidate)
                        except:
                            try:
                                candidate = db[MONGOD_COLLECTION].find({'_id':ObjectId(ids2[k])})[0]['keyword']
                                if(candidate == keyword1[count]):
                                    candidates.append(candidate)
                                else:
                                    candidates.append(keyword1[count])
                                    candidates.append(candidate)
                            except:
                                continue
                        
                        try:
                            origin = db[MONGOD_COLLECTION].find({'_id':ObjectId(ids2[k])})[0]['refer']
                            if(origin !=''):
                                db[MONGOD_COLLECTION].update({'_id':ObjectId(ids1[count])},{'$set':{'duplicate':'true','refer':origin,'candidate':candidates}})
                            else:
                                db[MONGOD_COLLECTION].update({'_id':ObjectId(ids1[count])},{'$set':{'duplicate':'true','refer':ids2[k],'candidate':candidates}})
                        except:
                            db[MONGOD_COLLECTION].update({'_id':ObjectId(ids1[count])},{'$set':{'duplicate':'true','refer':ids2[k],'candidate':candidates}})
                    elif(float(times1[count])<float(times2[k])):
                        candidates = []
                        try:
                            candidate = db[MONGOD_COLLECTION].find({'_id':ObjectId(ids1[count])})[0]['candidate']
                            if isinstance(candidate,list):
                                candidates = candidate
                                if keyword2[k] not in candidate:
                                    candidates.append(keyword2[k])
                            elif(candidate == keyword2[k]):
                                candidates.append(keyword2[k])
                            else:
                                candidates.append(keyword2[k])
                                candidates.append(candidate)
                            
                        except:
                            try:
                                candidate = db[MONGOD_COLLECTION].find({'_id':ObjectId(ids1[count])})[0]['keyword']
                                if(candidate == keyword2[k]):
                                    candidates.append(candidate)
                                else:
                                    candidates.append(keyword2[k])
                                    candidates.append(candidate)
                            except:
                                continue
                        try:
                            origin = db[MONGOD_COLLECTION].find({'_id':ObjectId(ids1[count])})[0]['refer']
                            if(origin !=''):
                                db[MONGOD_COLLECTION].update({'_id':ObjectId(ids2[k])},{'$set':{'duplicate':'true','refer':origin,'candidate':candidates}})
                            else:
                                db[MONGOD_COLLECTION].update({'_id':ObjectId(ids2[k])},{'$set':{'duplicate':'true','refer':ids1[count],'candidate':candidates}})
                        except:
                            db[MONGOD_COLLECTION].update({'_id':ObjectId(ids2[k])},{'$set':{'duplicate':'true','refer':ids1[count],'candidate':candidates}})
                       
                        
                    elif(float(times1[count])==float(times2[k])):
                        if(float(last1[count])>float(last2[k])):
                            
                            candidates = []
                            try:
                                candidate = db[MONGOD_COLLECTION].find({'_id':ObjectId(ids2[k])})[0]['candidate']
                                if isinstance(candidate,list):
                                    candidates = candidate
                                    if keyword1[count] not in candidate:
                                        candidates.append(keyword1[count])
                                elif(candidate == keyword1[count]):
                                    candidates.append(keyword1[count])
                                else:
                                    candidates.append(keyword1[count])
                                    candidates.append(candidate)
                            except:
                                try:
                                    candidate = db[MONGOD_COLLECTION].find({'_id':ObjectId(ids2[k])})[0]['keyword']
                                    if(candidate == keyword1[count]):
                                        candidates.append(candidate)
                                    else:
                                        candidates.append(candidate)
                                        candidates.append(keyword1[count])
                                except:
                                    continue
                            try:
                                origin = db[MONGOD_COLLECTION].find({'_id':ObjectId(ids2[k])})[0]['refer']
                                if(origin !=''):
                                    db[MONGOD_COLLECTION].update({'_id':ObjectId(ids1[count])},{'$set':{'duplicate':'true','refer':origin,'candidate':candidates}})
                                else:
                                    db[MONGOD_COLLECTION].update({'_id':ObjectId(ids1[count])},{'$set':{'duplicate':'true','refer':ids2[k],'candidate':candidates}})
                            except:
                                db[MONGOD_COLLECTION].update({'_id':ObjectId(ids1[count])},{'$set':{'duplicate':'true','refer':ids2[k],'candidate':candidates}})
                        else:
                            candidates = []
                            try:
                                candidate = db[MONGOD_COLLECTION].find({'_id':ObjectId(ids1[count])})[0]['candidate']
                                if isinstance(candidate,list):
                                    candidates = candidate
                                    if keyword2[k] not in candidate:
                                        candidates.append(keyword2[k])
                                elif(candidate == keyword2[k]):
                                    candidates.append(keyword2[k])
                                else:
                                    candidates.append(keyword2[k])
                                    candidates.append(candidate)
                            except:
                                try:
                                    candidate = db[MONGOD_COLLECTION].find({'_id':ObjectId(ids1[count])})[0]['keyword']
                                    if(candidate == keyword2[k]):
                                        candidates.append(candidate)
                                    else:
                                        candidates.append(candidate)
                                        candidates.append(keyword2[k])
                                except:
                                    continue
                            try:
                                origin = db[MONGOD_COLLECTION].find({'_id':ObjectId(ids1[count])})[0]['refer']
                                if(origin !=''):
                                    db[MONGOD_COLLECTION].update({'_id':ObjectId(ids2[k])},{'$set':{'duplicate':'true','refer':origin,'candidate':candidates}})
                                else:
                                    db[MONGOD_COLLECTION].update({'_id':ObjectId(ids2[k])},{'$set':{'duplicate':'true','refer':ids1[count],'candidate':candidates}})
                            except:
                                db[MONGOD_COLLECTION].update({'_id':ObjectId(ids2[k])},{'$set':{'duplicate':'true','refer':ids1[count],'candidate':candidates}})
            k += 1
                    
        count += 1
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='news.log',
                filemode='a')
    start_cal = str(daysbefore)
    #start_cal = '2015-06-01'
    end_cal = str(tomorrow)
    start_time = time.time()
    print "start shingle_news"
    logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='news.log',
                filemode='a')
    logging.info(' start shingle_news:' + str(ts2HMS_1(time.time())) )
    shingle_news(start_cal, end_cal)
    logging.info(' end shingle_news:' + str(ts2HMS_1(time.time())) )
    #shingle_news()
    end_time = time.time()
    ca_time = end_time - start_time
    logging.info('total cal_time shingle_news :'+ str(ca_time)  )
    print "shingle_news cal_time:"+str(ca_time)

    

