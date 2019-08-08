# -*- coding: utf-8 -*-

#單月資料
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
from app.models import Monthlydata
from celery import shared_task, task
import random

delay_list = [3, 4, 5]
srr = random.SystemRandom()

@task(name='monthly_stock')
def monthly_stock():
    tyear = datetime.datetime.now().year
    tmonth = datetime.datetime.now().month
    tday =  datetime.datetime.now().date()
    for x in range(1):

        wait_t = srr.choice(delay_list)    
        time.sleep(wait_t)

        ye = tyear - x -1911
        ye2 = tyear - x
        for z in range(1, tmonth):
            mo = tmonth - z
            url = 'https://mops.twse.com.tw/nas/t21/sii/t21sc03_'+str(ye)+'_'+str(mo)+'_0.html'
    
            req= requests.get(url)
            soup = BeautifulSoup(req.text, 'html.parser')
            table = soup.find_all('table')
            req.encoding = 'big5'
            if table:
                dfs = pd.read_html(req.text)
                for r in range(2, len(dfs), 2):
                    df = dfs[r]
                    if len(df)<2:
                        pass
                    else:
                        df = df.iloc[:,0:10]
                        df.columns= [u'證券代號', u'證券名稱', u'當月營收', u'上月營收', u'去年當月營收', u'上月比較增減(%)', u'去年同月增減(%)', u'當月累計營收', u'去年累計營收', u'前期比較增減(%)']
                        row_length = len(df)-1
                        for y in range(row_length):
                            s_id=df[u'證券代號'].iloc[y]
                            if Monthlydata.objects.filter(stockid=s_id, year_n=ye2, month_n=mo).exists():
                                pass
                            else:
                                monthlyd = Monthlydata (
                                    stockid = s_id,
                                    stockname = df[u'證券名稱'].iloc[y],
                                    revenue = 1000*df[u'當月營收'].iloc[y],
                                    last_revenue = 1000*df[u'上月營收'].iloc[y],
                                    last_revenue_p = df[u'上月比較增減(%)'].iloc[y],
                                    last_year_revenue = 1000*df[u'去年當月營收'].iloc[y],
                                    last_year_revenue_p = df[u'去年同月增減(%)'].iloc[y],      
                                    acc_revenue = 1000*df[u'當月累計營收'].iloc[y],
                                    last_year_acc_revenue = 1000*df[u'去年累計營收'].iloc[y],
                                    acc_revenue_p = df[u'前期比較增減(%)'].iloc[y],
                                    year_n = ye2,
                                    month_n = mo

                                    )
                                monthlyd.save()

                print(str(ye) + '-'+str(mo)+ '- saved')                

            
            else:
                print(str(ye) + '-'+str(mo)+ '- not valid')



