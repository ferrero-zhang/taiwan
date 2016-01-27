# -*- coding: utf-8 -*-

import time
import datetime
import re

def date4stamp(date):
    md = date.split(' ')
    mmdd = md[0].split(u'月')
    mth = mmdd[0]
    dy = mmdd[1].split(u'日')[0]
    if(int(mth)<12):
        mmdd = '2016-' + str(mth)+'-'+str(dy)
    else:
        mmdd = '2015-' + str(mth)+'-'+str(dy)
    hm = md[2]
    hm = str(hm) + ':00'
    timestamp = mmdd + ' '+ hm
    #print timestamp
    timestamp =int(time.mktime(time.strptime(timestamp,'%Y-%m-%d %H:%M:%S')))
    return timestamp



