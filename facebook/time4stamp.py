# -*- coding: utf-8 -*-

import time
import datetime
import re

def time2stamp(timewords):
    
    dt = datetime.datetime.now()
    year = dt.year
    month = dt.month
    day = dt.day
    hour = dt.hour
    minute = dt.minute
    words = timewords.split(' ')
    ws = words[0].encode('utf-8')
    ts = words[1]
    dayt = ts.split(':')
    day2time = ''
    weekd = 0
    try:
        if '上' in ws:
            day2time = str(dayt[0]) + ':' +str(dayt[1]) + ':00'
        elif '下' in ws:
            aft = int(dayt[0])+int(12)
            if aft>=24:
                aft = int(dayt[0])
            day2time = str(aft) + ':' +str(dayt[1]) + ':00'
    except:
        day2time = '00:00:00'
    
    if '月' in ws:

        yue = ws.split('月')
        try:
            get_month = yue[0].split('年')[1]
            year = yue[0].split('年')[0]
        except:
            get_month = yue[0]
        ri = yue[1].split('日')
        get_day = ri[0]
        if(day<int(get_day)):
            day = day+30
        if(day-int(get_day)>7):
            day2time = '00:00:00'
        if day2time:
            day2time = day2time
        else:
            day2time = '00:00:00'                             
        if (int(get_month)<10):
            if(int(get_day)<10):
                timest = str(year) + '-0' + str(get_month) + '-0' + str(get_day) + ' ' + day2time
            else:
                timest = str(year) + '-0' + str(get_month) + '-' + str(get_day) + ' ' + day2time
        elif(int(get_day)<10):
            timest = str(year) + '-' + str(get_month) + '-0' + str(get_day) + ' ' + day2time
        else:
            timest = str(year) + '-' + str(get_month) + '-' + str(get_day) + ' ' + day2time
        
    elif '周' in ws:

        cwd = datetime.date.today().weekday()+1
        zhou = re.search(r'周(.*?)午',ws)
        t_zhou = zhou.group(1)
        get_zhou = t_zhou[0:3]
        if(get_zhou == '一'):
            weekd = 1
        elif(get_zhou == '二'):
            weekd =2
        elif(get_zhou == '三'):
            weekd =3
        elif(get_zhou == '四'):
            weekd =4
        elif(get_zhou == '五'):
            weekd =5
        elif(get_zhou == '六'):
            weekd =6
        elif(get_zhou == '日'):
            weekd =7
        get_day_w = day -abs(cwd-weekd)

        if(get_day_w<1):
            month = month -1
            get_day_w = day_of_month(month) + get_day_w
            if (month<10):
                if(int(get_day_w)<10):
                    timest = str(year) + '-0' + str(month) + '-0' + str(get_day_w) + ' ' + day2time
                else:
                    timest = str(year) + '-0' + str(month) + '-' + str(get_day_w) + ' ' + day2time
            elif(int(get_day_w)<10):
                timest = str(year) + '-' + str(month) + '-0' + str(get_day_w) + ' ' + day2time
            else:
                timest = str(year) + '-' + str(month) + '-' + str(get_day_w) + ' ' + day2time
        elif(month<10):
            if(int(get_day_w)<10):
                timest = str(year) + '-0' + str(month) + '-0' + str(get_day_w) + ' ' + day2time
            else:
                timest = str(year) + '-0' + str(month) + '-' + str(get_day_w) + ' ' + day2time
        elif(int(get_day_w)<10):
            timest = str(year) + '-' + str(month) + '-0' + str(get_day_w) + ' ' + day2time
        else:
            timest = str(year) + '-' + str(month) + '-' + str(get_day_w) + ' ' + day2time
        
    elif '昨' in ws:
 
        get_day_d = day - 1
        if(get_day_d<1):
            month = month -1
            if(month<1):
                year = year - 1
                month = month + 12
            get_day_d = day_of_month(month) + get_day_d
            
            if (month<10):
                if(int(get_day_d)<10):
                    timest = str(year) + '-0' + str(month) + '-0' + str(get_day_d) + ' ' + day2time
                else:
                    timest = str(year) + '-0' + str(month) + '-' + str(get_day_d) + ' ' + day2time
            elif(int(get_day_d)<10):
                timest = str(year) + '-' + str(month) + '-0' + str(get_day_d) + ' ' + day2time
            else:
                timest = str(year) + '-' + str(month) + '-' + str(get_day_d) + ' ' + day2time
        elif(month<10):
            if(int(get_day_d)<10):
                timest = str(year) + '-0' + str(month) + '-0' + str(get_day_d) + ' ' + day2time
            else:
                timest = str(year) + '-0' + str(month) + '-' + str(get_day_d) + ' ' + day2time
        elif(int(get_day_d)<10):
            timest = str(year) + '-' + str(month) + '-0' + str(get_day_d) + ' ' + day2time
        else:
            timest = str(year) + '-' + str(month) + '-' + str(get_day_d) + ' ' + day2time
        
    elif '今' in ws:
        daytime = ts.split(':')
        hh = daytime[0]
        mm = daytime[1]
        if(int(hh)<10):
            hh = '0' + hh
        timest = str(year) +'-'+ str(month) + '-' + str(day) + ' '+ hh + ':' + mm +':00'
        print timest
    else:

        words = timewords.encode('utf-8')
        hours = words.split(' ')
        if '分' in words:
            h = hour - int(1)
        else:
            h = hour - int(ws)
        if(h<0):
            h = 24 + h
            day = day - 1
        if(day<1):
            day = day_of_month(month) + day
            month = month - 1
        if(month<1):
            year = year - 1
            month = month + 12
        if (month<10):
            if(day<10):
                timest = str(year) +'-0'+ str(month) + '-0' + str(day) + ' '+ str(h) + ':00:00'
            else:
                timest = str(year) +'-0'+ str(month) + '-' + str(day) + ' '+ str(h) + ':00:00'
        elif(day<10):
            timest = str(year) +'-'+ str(month) + '-0' + str(day) + ' '+ str(h) + ':00:00'
        else:
            timest = str(year) +'-'+ str(month) + '-' + str(day) + ' '+ str(h) + ':00:00'
        '''
        if '时' in ts:  
            print ts
            words = timewords.encode('utf-8')
            hours = words.split(' ')
            h = hour - int(ws)
            if(h<0):
                h = 24 + h
                day = day - 1
            if(day<0):
                day = day_of_month(month) + day
                month = month - 1
            if (month<10):
                if(day<10):
                    timest = str(year) +'-0'+ str(month) + '-0' + str(day) + ' '+ str(h) + ':00:00'
                else:
                    timest = str(year) +'-0'+ str(month) + '-' + str(day) + ' '+ str(h) + ':00:00'
            elif(day<10):
                timest = str(year) +'-'+ str(month) + '-0' + str(day) + ' '+ str(h) + ':00:00'
            else:
                timest = str(year) +'-'+ str(month) + '-' + str(day) + ' '+ str(h) + ':00:00'
        #timest = str(year) +'-'+ str(month) + '-' + str(day) + ' '+ str(h) + ':00:00'
        elif '分' in ts:  
            print ws
            mit = minute - int(ws)
            if(mit<0):
                mit = 60 + mit
                hour = hour - 1
            if(hour<0):
                hour = 24 + hour
                day = day - 1
            if(day<0):
                day = day_of_month(month) + day
                month = month - 1
            
            timest = str(year) +'-'+ str(month) + '-' + str(day) + ' '+ str(h) + ':' +str(mit) + ':00'
            '''
    print timest
    return timest

def day_of_month(month):

    dt = datetime.datetime.now()
    year = dt.year
    if (month==1 or month==3 or month==5 or month==7 or month==8 or month==10 or month==12):
        daymonth = 31
    elif(month==4 or month==6 or month==9 or month==11):
        daymonth = 30
    elif(year%4):
        daymonth = 29
    else:
        daymonth = 28
    
    return daymonth  

