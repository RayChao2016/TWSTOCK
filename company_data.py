
#公司資料
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
from app.models import Quarterdata, Dailydata, Cdata
from celery import shared_task, task
import random


delay_list = [3, 4]
srr = random.SystemRandom()
tday =  datetime.datetime.now().date()
theday =  datetime.datetime.now().date()- timedelta(days=6)
@task(name='company_data')
def company_data():
#using 2019/8/2 data finished
    if Dailydata.objects.filter(data_date = tday).exists():
        the_day = tday
    else:
        the_day = theday

    for item in Dailydata.objects.filter(data_date=the_day):
        wait_t = srr.choice(delay_list)    
        time.sleep(wait_t)
        s_id=item.stockid
        stock=item.stockname
        url = 'https://histock.tw/stock/profile.aspx?no='+s_id+'&t=6'      #公司資料


        req= requests.get(url)
        soup = BeautifulSoup(req.text, 'html.parser')
        table = soup.find_all('table')
        table_body = soup.find_all('tr')
        table_body2 = soup.find_all('td')
        empty_table = soup.find("td", text="無該公司資料")
        #req.encoding = 'big5'
        if table and table_body and table_body2:
            if not empty_table:
                dfs = pd.read_html(req.text)
                time.sleep(1)
                df=dfs[1]

                c_data = Cdata (
                    stockid = s_id,                                       #證券代號
                    stockname = stock,                                    #證券名稱
                    capital = df.iloc[0, 4],                              #實收資本額     
                    total_share = df.iloc[0, 5],                          #已發行普通股
                    pe_ratio = df.iloc[2, 1],                             #本益比
                    pb_ratio = df.iloc[2, 4],                             #股價淨值比
                    d_yield = df.iloc[2, 5][:-1],                         #現金殖利率 (%)
                    build_date = df.iloc[0, 0]                            #成立日期
                )
                c_data.save()             
                print(s_id + ', saved')       
            else:
                print(s_id + ', no data')
        else:
            print(s_id + ', no data')
