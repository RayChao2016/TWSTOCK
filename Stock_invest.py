# -*- coding: utf-8 -*-

#投信
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
from app.models import Investdata
from celery import shared_task, task
import random

delay_list = [3, 4, 5]
srr = random.SystemRandom()

@task(name='daily_invest')
def daily_invest():
    for x in range(0, 100):
        wait_t = srr.choice(delay_list)    
        time.sleep(wait_t)
        theday =  datetime.datetime.now().date()- timedelta(days=x)
        theday_s = theday.strftime("%Y%m%d")       

        url = 'https://www.twse.com.tw/fund/TWT44U?response=html&date=' + theday_s
    
        doc = requests.get(url)
        soup = BeautifulSoup(doc.text, 'html.parser')
        table = soup.find_all('table')
        if table:
            dfs = pd.read_html(url)  ## 回傳DataFrame類別的陣列
            df = dfs[0]
            df = df.iloc[:,1:6]

            df.columns = [ u'證券代號', u'證券名稱', u'買進股數', u'賣出股數',  u'買賣超股數']
            row_length = len(df)-1
            for y in range(row_length):

                s_id=df[u'證券代號'].iloc[y]

                if Investdata.objects.filter(stockid=s_id, data_date=theday).exists():
                    pass
                else:

                    dailyi = Investdata (
                        stockid = s_id,
                        stockname = df[u'證券名稱'].iloc[y],
                        buy_share = df[u'買進股數'].iloc[y],
                        sell_share = df[u'賣出股數'].iloc[y],
                        delta_share = df[u'買賣超股數'].iloc[y],
                        data_date = theday
                    )
                    dailyi.save()

            print(theday_s + '- saved')
        else:
            print(theday_s + '- not valid')

