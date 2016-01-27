 # -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import csv
import json
import time
import pymongo
import logging
from items import TwitterItem
from twisted.internet.threads import deferToThread


#MONGOD_HOST = '113.10.156.125'
MONGOD_HOST = 'localhost'
MONGOD_PORT = 27017
MONGOD_DB = 'taiwan'
MONGOD_COLLECTION = 'twitter'

def _default_mongo(host=MONGOD_HOST, port=MONGOD_PORT, usedb=MONGOD_DB):
    # 强制写journal，并强制safe
    connection = pymongo.MongoClient(host=host, port=port, j=False, w=1)
    db = connection.admin
    # db.authenticate('root', 'root')
    db = getattr(connection, usedb)
    return db

class TwitterPipeline(object):
    def __init__(self):
        self.file = open('items.jl', 'ab')

    def process_item(self, item, spider):
        line = json.dumps(item.to_dict()) + "\n"
        self.file.write(line)
        return item



class MongodbPipeline(object):
    def __init__(self, db, host, port, collection):
        self.db_name = db
        self.host = host
        self.port = port
        self.db = _default_mongo(host, port, usedb=db)
        self.collection = collection

        #logging.info('Mongod connect to {host}:{port}:{db}:{collection}'.format(host=host, port=port, db=db, collection=collection), level=logging.INFO)
    
    @classmethod
    def from_settings(cls, settings):
        db = settings.get('TWITTER_DB', None)
        host = settings.get('TWITTER_HOST', None)
        port = settings.get('TWITTER_PORT', None)
        collection = settings.get('TWITTER_COLLECTION', None)
        return cls(db, host, port, collection)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)
    
    def process_item(self, item, spider):
        if isinstance(item, TwitterItem):
            return deferToThread(self.process_twitter, item, spider)

    def update_twitter(self, collection, twitter_item):
        updates = {}
        updates['last_modify'] = time.time()
        for k, v in twitter_item.iteritems():
            updates[k] = v

        updates_modifier = {'$set': updates}
        self.db[collection].update({'_id': twitter_item['id_tweet']}, updates_modifier)


    def process_twitter(self, item, spider):
        twitter_item = item.to_dict()

        hit = False
        if twitter_item['id_tweet']:
            twitter_item['_id'] = twitter_item['id_tweet']

            if self.db[self.collection].find({'_id': twitter_item['id_tweet']}).count():
                hit = True

        if hit:
            self.update_twitter(self.collection, twitter_item)
        else:
            try:
                twitter_item['first_in'] = time.time()
                twitter_item['last_modify'] = twitter_item['first_in']
                self.db[self.collection].insert(twitter_item)
            except pymongo.errors.DuplicateKeyError:
                self.update_twitter(self.collection, twitter_item)

        return item 

    
