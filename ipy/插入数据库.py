#!/usr/bin/env python
# coding: utf-8

import os
import re
import numpy as np
import pandas as pd
from updatedb import UpdateDB
from sqlalchemy import create_engine
import scrapy
import requests
from scrapy.selector import Selector
import math
import os
from time import sleep
import datetime


def loadfiles(symbol,dir_path):
    files = os.listdir('%s/%s/' % (dir_path,symbol))
    news = []
    for f in files:
        try:
            news_f = pd.read_csv('%s/%s/%s' % (dir_path,symbol,f), dtype={'date':str})#[['date','title','time','url',conttent]]
            news.append(news_f)
        except:
            continue
    if len(news) > 0:
        news = pd.concat(news)
        news['URL'] = [url.split('/')[-1].split('.')[0] for url in news['url']]
        news = news.drop_duplicates('URL',keep='last').drop(['url'],axis=1)
        if len(news) > 0:
            return news
    return None

engine = create_engine('mysql://fineng:123456@10.24.224.249/wind?charset=utf8')

today = datetime.datetime.now()
date_begin = (today + datetime.timedelta(days=-90)).strftime('%Y%m%d')
date_end = (today + datetime.timedelta(days=30)).strftime('%Y%m%d')
index = pd.read_sql('select TRADE_DAYS from MyAShareCalendar where S_INFO_EXCHMARKET="SSE" order by TRADE_DAYS',engine).rename(columns={'TRADE_DAYS':'TRADE_DT'})
index['date'] = index['TRADE_DT']
all_date = pd.DataFrame({'date':[str(d)[:10].replace('-','') for d in pd.date_range(date_begin,date_end)]})
all_date = all_date.merge(index[['date','TRADE_DT']],how='left')
all_date['TRADE_DT'] = all_date['TRADE_DT'].bfill()
all_date['next_date'] = all_date['TRADE_DT'].shift(-1)
all_date['next_date'] = all_date['date'].shift(-1)

updb = UpdateDB('10.24.224.249','fineng','123456')
index = updb.pull_index(['000300.SH']).loc['20100101':].reset_index().rename(columns={'TRADE_DT':'date'}).sort_values('date')
all_date = pd.DataFrame({'date':[str(d)[:10].replace('-','') for d in pd.date_range('20100101',index['date'].values[-1])]})
index['TRADE_DT'] = index['date']
all_date = all_date.merge(index[['date','TRADE_DT']],how='left')
all_date['TRADE_DT'] = all_date['TRADE_DT'].bfill()
all_date['next_date'] = all_date['TRADE_DT'].shift(-1)
all_date['next_date'] = all_date['date'].shift(-1)

newsfiles = os.listdir('../news2')
newsfiles = list(filter(lambda x: x.split('.')[1][0]=='S', newsfiles))
print(newsfiles[-1])
engine = create_engine('mysql://wy:,.,.,l@10.24.224.249/webdata?charset=utf8')
for i,nf in enumerate(newsfiles[-1:]):
    # nf='000001.SZ'
    if (len(nf)==9):
        print('processing %s %d/%d' % (nf, i, len(newsfiles)))
        news = loadfiles(nf,'../news2')
        if news is None:
            continue
        # news = pd.read_csv('news/%s.csv' % symbol,dtype={'date':str})[['date','title','time']]

        news = news.dropna()
        news = news.sort_values('date')
        # 剔除无用消息
#         nonsense=['融资融券信息','融资净偿还','融资净买入','融券净偿还','大宗交易','今日超大单流','龙虎榜','下跌','上涨','跌幅','涨幅','涨停','跌停',
#                   '大涨','大跌','跳水','盘中','融资余额','反弹','回调','火箭发射','投资者关系']
#         is_useful = np.array([not any(bool(re.search(w,l)) for w in nonsense) for l in news['title'].values])
#         news = news[is_useful]
        news['date'] = [st.replace('-','') for st in news['date']]
        news['read'] = news['read'].astype(str)
        news['read'] = [int((int(bool(re.search('万',st)))*9999+1)*float(st.replace('万',''))) for st in news['read']]
        news['S_INFO_WINDCODE'] = nf
        
        news = news.merge(all_date[['date','next_date']],on='date',how='left')
        news['date2'] = news['date'].values
        news.loc[news['time']>'15:00:00','date2'] = np.nan
        news['date2'] = news['date2'].fillna(news['next_date'])
        news = news.merge(all_date[['date','TRADE_DT']].rename(columns={'date':'date2'}),how='left')
        news = news.drop(['next_date','date2'],axis=1)
        news = news.rename(columns={'date':'DATE','read':'READNUM','comment':'COMMENTNUM','title':'TITLE','content':'CONTENT','time':'TIME'})
        for j in range(len(news)):
            try:
                news.iloc[[j]].to_sql(name='EastMoney', con=engine, if_exists='append', index=False)
            except:
                pass
