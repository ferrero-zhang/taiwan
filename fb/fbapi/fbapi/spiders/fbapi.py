# -*- coding:utf-8 -*-

"""cmt_list_spider"""

import csv
import logging
#from scrapy import log
import simplejson as json
from scrapy.http import Request
from scrapy.conf import settings
from scrapy.spiders import BaseSpider, Spider
from fbapi.utils import time2stamp

BASE_URL = 'https://graph.facebook.com/v2.5/{postId}/comments?summary=1&access_token='+ACCESS_TOKEN'


class CmtListSpider(Spider):
    """usage: scrapy crawl cmt_list_spider -a weibo_id=weibo_id.txt
    """
    name = 'cmt_list_spider'



    #def __init__(self, weibo_id):
    def __init__(self):
        #self.weibo_id = weibo_id
        pass

    def start_requests(self):
        id_list = self.prepare()
        for i in range(0,len(id_list)):
            #print i
            mid = id_list[i]
            request = Request(BASE_URL.format(postId=mid))
            request.meta['postId'] = mid
            yield request

    def parse(self, response):

        m_id = response.meta['postId']
        resp = json.loads(response.body)
        results = []
        if resp.get('statuses') == []:
            fw.write(str(tweetId)+'\n')
        #    raise ShouldNotEmptyError()
        if not resp['comments']:
            return results

        for status in resp['comments']:
            items = resp2item_search(status, search_type=CmtListSpider.SEARCH_TYPE)
            results.extend(items)


        page += 1
        request = Request(BASE_URL.format(tweetId=tweetId,page=page), headers=None)
        request.meta['page'] = page
        request.meta['tweetId'] = tweetId

        results.append(request)

        return results

    def prepare(self):
        reader = open('m_id.txt', 'rb')
        for line in reader:
            line = line.strip()
            id_list.append(line)

        return id_list
