# -*- coding: utf-8 -*-

#單日股價
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import os
import json
import time
import datetime
from datetime import date, timedelta
from django.db.models import Q
from app.models import Dailydata
from celery import shared_task, task
import random

delay_list = [3, 4, 5]
srr = random.SystemRandom()

@task(name='daily_test')
def daily_test():
    print('test')

@task(name='daily_stock')
def daily_stock():
    for x in range(97, 200):

        wait_t = srr.choice(delay_list)    
        time.sleep(wait_t)

        theday =  datetime.datetime.now().date()- timedelta(days=x)
        theday_s = theday.strftime("%Y%m%d")       

        url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&date=' + theday_s + '&type=ALLBUT0999'
    
        doc = requests.get(url)
        soup = BeautifulSoup(doc.text, 'html.parser')
        table = soup.find_all('table')

        if table:
            dfs = pd.read_html(doc.text)  ## 回傳DataFrame類別的陣列
            df = dfs[8]

            df = df.iloc[:,0:11]
            df.columns= [u'證券代號', u'證券名稱', u'成交股數', u'成交筆數', u'成交金額', u'開盤價', u'最高價', u'最低價', u'收盤價', u'漲跌(+/-)', u'漲跌價差']
            row_length = len(df)
            print(row_length)
            for y in range(row_length):
                s_id=df[u'證券代號'].iloc[y]
                if df[u'成交金額'].iloc[y] == 0 and df[u'成交股數'].iloc[y] == 0:
                    pass
                if df[u'收盤價'].iloc[y]== '--':
                    pass

                if df[u'最高價'].iloc[y]== '--':
                    pass
                else:
                    if Dailydata.objects.filter(stockid=s_id, data_date=theday).exists():
                        pass
                    else:
                        if df[u'漲跌(+/-)'].iloc[y] == '-':
                            daily_change = (-1)*df[u'漲跌價差'].iloc[y]

                        else:
                            daily_change = df[u'漲跌價差'].iloc[y]   

                        dailyd = Dailydata (
                            stockid = s_id,
                            stockname = df[u'證券名稱'].iloc[y],
                            trans_share = df[u'成交股數'].iloc[y],
                            trans_volume = df[u'成交筆數'].iloc[y],
                            trans_amount = df[u'成交金額'].iloc[y],
                            open_price = df[u'開盤價'].iloc[y],
                            dayhigh_price = df[u'最高價'].iloc[y],
                            daylow_price = df[u'最低價'].iloc[y],
                            close_price = df[u'收盤價'].iloc[y],
                            data_date = theday,
                            change = daily_change
                            )
                        dailyd.save()

            print(theday_s + '- saved')

        else:
            print(theday_s + '- not valid')


