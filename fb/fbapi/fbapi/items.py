# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class FbapiItem(scrapy.Item):
    # define the fields for your item here like:
    m_id = scrapy.Field()
    user_name = scrapy.Field()
    user_id = scrapy.Field()
    comment_message = scrapy.Field()
    created_time = scrapy.Field()
    timestamp = scrapy.Field()
    comment_count = scrapy.Field()
    author = scrapy.Field()
    
    RESP_TWEET_WEB = ['m_id','user_name','user_id','comment_message','created_time','timestamp','comment_count','author']

    def to_dict(self):
        d = {}
        for k, v in self.items():
            if isinstance(v, FbapiItem):
                d[k] = v.to_dict()
            else:
                d[k] = v
        return d

