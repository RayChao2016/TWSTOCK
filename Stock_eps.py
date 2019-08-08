

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
from app.models import Quarterdata, Dailydata
from celery import shared_task, task
import random


delay_list = [3, 4]
srr = random.SystemRandom()
tday =  datetime.datetime.now().date()
theday =  datetime.datetime.now().date()- timedelta(days=6)
@task(name='quarterly_data')
def quarterly_data():

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
#url = 'https://histock.tw/stock/profile.aspx?no=2330&t=6'      #公司資料
        url = 'https://histock.tw/stock/financial.aspx?no='+s_id+'&st=2'   #eps
        url_2 = 'https://histock.tw/stock/financial.aspx?no='+s_id+'&st=3'   #每股淨值
        url_3 = 'https://histock.tw/stock/financial.aspx?no='+s_id+'&st=4'   #損益表

    #url
        req= requests.get(url)
        soup = BeautifulSoup(req.text, 'html.parser')
        table = soup.find_all('table')
        table_body = soup.find_all('tr')
    #req.encoding = 'big5'
        if table and table_body:
            time.sleep(1)
            dfs = pd.read_html(req.text)
            df=dfs[0]
            for col in df.columns: 
                if len(col)==4:
                    for z in range(4):
                        if Quarterdata.objects.filter(stockid=s_id, year_n= col, quarter_n=z+1).exists():
                            qdata=Quarterdata.objects.get(stockid=s_id, year_n= col, quarter_n=z+1)               
                            if qdata.eps:
                                pass
                            else:
                                if df[col].iloc[z] == '-':
                                    pass
                                else:
                                    qdata.eps=float(df[col].iloc[z])
                                    qdata.save(update_fields=['eps'])
                                    print(col + '/Q' + str(z+1) + '-'+s_id + ', eps')
                        else:
                            if df[col].iloc[z] == '-':
                                pass                        
                            else:
                                qudata = Quarterdata (
                                    stockid = s_id,
                                    stockname = stock,
                                    eps = float(df[col].iloc[z]),
                                    year_n= col,
                                    quarter_n = z+1
                                )
                                qudata.save()
                                print(col + '/Q' + str(z+1) + '-'+s_id + ', eps')
                else:
                    print(s_id + ', eps no data')                    
        else:
            print(s_id + ', eps not valid')

 
    #url_2
        req_2= requests.get(url_2)
        soup_2 = BeautifulSoup(req_2.text, 'html.parser')
        table_2 = soup_2.find_all('table')
        table_body_2 = soup_2.find_all('tr')
    #req.encoding = 'big5'
        if table_2 and table_body_2:
            time.sleep(1)
            dfs = pd.read_html(req_2.text)
            df=dfs[0]
            for col in df.columns: 
                if len(col)==4:
                    for z in range(4):
                        if Quarterdata.objects.filter(stockid=s_id, year_n= col, quarter_n=z+1).exists():
                            qdata=Quarterdata.objects.get(stockid=s_id, year_n= col, quarter_n=z+1)               
                            if qdata.networth:
                                pass
                            else:
                                if df[col].iloc[z] == '-':
                                    pass
                                else:
                                    qdata.networth=float(df[col].iloc[z])
                                    qdata.save(update_fields=['networth'])
                                    print(col + '/Q' + str(z+1) + '-'+s_id + ', networth')
                        else:
                            if df[col].iloc[z] == '-':
                                pass                        
                            else:
                                qudata = Quarterdata (
                                    stockid = s_id,
                                    stockname = stock,
                                    networth = float(df[col].iloc[z]),
                                    year_n= col,
                                    quarter_n = z+1
                                )
                                qudata.save()
                                print(col + '/Q' + str(z+1) + '-'+s_id + ', networth')
                else:
                    print(s_id + ', networth no data')         
        else:
            print(s_id + ', networth not valid')


    #url_3
        req_3= requests.get(url_3)
        soup_3 = BeautifulSoup(req_3.text, 'html.parser')
        table_3 = soup_3.find_all('table')
        table_body_3 = soup_3.find_all('tr')
    #req.encoding = 'big5'
        if table_3 and table_body_3:
            time.sleep(1)
            dfs = pd.read_html(req_3.text)
            df=dfs[0]
            df.columns = [ u'季別', u'營收', u'毛利', u'營業利益', u'稅前淨利',  u'稅後淨利']  
            row_length = len(df)
            for y in range(row_length):
                yr=df[u'季別'].iloc[y][0:4]
                q_n=df[u'季別'].iloc[y][-1]
                if Quarterdata.objects.filter(stockid=s_id, year_n= yr, quarter_n=q_n).exists():
                    qdata=Quarterdata.objects.get(stockid=s_id, year_n= yr, quarter_n=q_n)               
                    if qdata.revenue and qdata.profit and qdata.op_income and qdata.pre_t_income and qdata.net_income:
                        pass
                    else:        
                        qdata.revenue = df[u'營收'].iloc[y]
                        qdata.profit = df[u'毛利'].iloc[y]
                        qdata.op_income = df[u'營業利益'].iloc[y]
                        qdata.pre_t_income = df[u'稅前淨利'].iloc[y]           
                        qdata.net_income = df[u'稅後淨利'].iloc[y]                   
                        qdata.save(update_fields=['revenue', 'profit', 'op_income', 'pre_t_income', 'net_income'])
                        print(yr + '/Q' + q_n + '-'+s_id + ', profit and loss')
                else:
                    qudata = Quarterdata (
                        stockid = s_id,
                        stockname = stock,
                        revenue = df[u'營收'].iloc[y],
                        profit = df[u'毛利'].iloc[y],
                        op_income = df[u'營業利益'].iloc[y],
                        pre_t_income = df[u'稅前淨利'].iloc[y],        
                        net_income = df[u'稅後淨利'].iloc[y],                    
                        year_n= yr,
                        quarter_n = q_n
                    )
                    qudata.save()
                    print(yr + '/Q' + q_n + '-'+s_id + ', profit and loss')

        else:
            print(s_id + ', profit and loss not valid')
