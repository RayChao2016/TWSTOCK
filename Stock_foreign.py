# -*- coding: utf-8 -*-

#外資及陸資
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
from app.models import Foreigndata
from celery import shared_task, task
import random

delay_list = [3, 4, 5]
srr = random.SystemRandom()

@task(name='daily_foreign')
def daily_foreign():
    tday =  datetime.datetime.now().date()

    for x in range(47, 150):
        wait_t = srr.choice(delay_list)    
        time.sleep(wait_t)
        theday =  tday- timedelta(days=x)

        theday_s = theday.strftime("%Y%m%d")       

        url = 'https://www.twse.com.tw/fund/TWT38U?response=html&date=' + theday_s
    
        doc = requests.get(url)
        time.sleep(0.2)
        soup = BeautifulSoup(doc.text, 'html.parser')
        time.sleep(0.2)
        table = soup.find_all('table')
        if table:
            dfs = pd.read_html(doc.text)  ## 回傳DataFrame類別的陣列
            df = dfs[0]
            df = df.iloc[:,[1,2,9,10,11]]

            df.columns = [ u'證券代號', u'證券名稱', u'買進股數', u'賣出股數',  u'買賣超股數']  
            row_length = len(df)
            for y in range(row_length):
                time.sleep(0.2)
                s_id=df[u'證券代號'].iloc[y]

                if Foreigndata.objects.filter(stockid=s_id, data_date=theday).exists():
                    pass
                else:   
                    df = df.copy()
                    #df[u'買進股數']= df[u'買進股數'].astype(int)
                    #df[u'賣出股數']=df[u'賣出股數'].astype(int)
                    #df[u'買賣超股數']=df[u'買賣超股數'].astype(int)

                    dailyf = Foreigndata (
                        stockid = s_id,
                        stockname = df[u'證券名稱'].iloc[y],
                        buy_share = df[u'買進股數'].iloc[y].astype(int),
                        sell_share = df[u'賣出股數'].iloc[y].astype(int),
                        delta_share = df[u'買賣超股數'].iloc[y].astype(int),
                        data_date = theday
                    )
                    dailyf.save()
                    print(s_id+ '- saved')

            print(theday_s + '- saved')

        else:
            print(theday_s + '- not valid')

