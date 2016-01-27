# -*- coding=utf-8 -*-


import logging
import re
import os
import csv
import time
from twisted.internet import task
from scrapy.mail import MailSender
from scrapy.exceptions import NotConfigured
from scrapy import signals

logger = logging.getLogger(__name__)


class LogStats(object):
    """Log basic scraping stats periodically"""

    def __init__(self, stats, interval, mailfrom, mailto, smtphost, smtpuser, smtppass, error_threshold):
        self.stats = stats
        self.interval = interval
        self.multiplier = 60.0 / self.interval
        
        self.mailfrom = mailfrom
        self.mailto = mailto
        self.smtphost = smtphost
        self.smtpuser = smtpuser
        self.smtppass = smtppass
        self.error_threshold = error_threshold
        
    @classmethod
    def from_crawler(cls, crawler):
        interval = crawler.settings.getfloat('LOGSTATS_INTERVAL')
        mailfrom = crawler.settings.get('WARNING_MAIL_FROM')
        mailto = crawler.settings.get('WARNING_MAIL_TO')
        smtphost = crawler.settings.get('WARNING_SMTP_HOST')
        smtpuser = crawler.settings.get('WARNING_SMTP_USER')
        smtppass = crawler.settings.get('WARNING_SMTP_PASS')
        error_threshold = crawler.settings.get('ERROR_THRESHOLD')
        if not interval:
            raise NotConfigured
        o = cls(crawler.stats, interval, mailfrom, mailto, smtphost, smtpuser, smtppass, error_threshold)
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o

    def spider_opened(self, spider):
        self.pagesprev = 0
        self.itemsprev = 0
        self.exception_countprev = 0
        self.pipelines_total_listcountprev = 0
        self.request_total_countprev = 0


        self.task = task.LoopingCall(self.log, spider)
        self.task.start(self.interval)



    def log(self, spider):

		
        items = self.stats.get_value('item_scraped_count', 0)
        pages = self.stats.get_value('response_received_count', 0)
        exception_count = self.stats.get_value('downloader/exception_count',0)
        
        irate = (items - self.itemsprev) * self.multiplier
        prate = (pages - self.pagesprev) * self.multiplier
		
        errrate = (exception_count - self.exception_countprev) * self.multiplier

        self.pagesprev, self.itemsprev = pages, items
        

        msg = ("Crawled "+str(pages)+" pages (at "+str(prate)+" pages/min), scraped "+str(items)+" items (at "+str(irate)+" items/min)")
        #log_args = {'pages': pages, 'pagerate': prate,'items': items, 'itemrate': irate}
        
        logging.info(msg)
        if errrate > self.error_threshold:
            mailer = MailSender(smtphost=self.smtphost, mailfrom=self.mailfrom, smtpuser=self.smtpuser, smtppass=self.smtppass)
            mailer.send(to=[self.mailto], subject="Scrapy twitter Error", body="Exception rate has reached to %d" % (errrate))


    def spider_closed(self, spider, reason):
        if self.task.running:
            self.task.stop()
