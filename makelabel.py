# coding: utf-8


import os
import sys
import re
import string
import pandas as pd
import numpy as np
from copy import deepcopy
from updatedb import UpdateDB
from collections import Counter, defaultdict
from sklearn.model_selection import train_test_split
from sklearn.utils import shuffle
# =========================================== 获得日期数据 ========================================================================

updb = UpdateDB('10.24.224.249','fineng','123456')
index = updb.pull_index(['000300.SH']).loc['20100101':].reset_index().rename(columns={'TRADE_DT':'date'}).sort_values('date')
index['next_date'] = index['date'].shift(-1)

code_name = updb.pull_data_accor_date('MyAShareDescription','wind',set_index=False).set_index('S_INFO_WINDCODE')
output_dir = 'output0331'
# =========================================== 处理新闻数据 ========================================================================

newsfiles = os.listdir('news2')
newsfiles = list(filter(lambda x: x.split('.')[1][0]=='S', newsfiles))

def get_price(symbol):
    # 计算股票的涨跌幅
    price = updb.pull_stocks([symbol], startT='20100101').sort_index().reset_index().rename(columns={'TRADE_DT':'date',symbol:'close'})
    price['CloseRet_t-2_t+1'] = price['close'].shift(-1) / price['close'].shift(2)-1
    price['CloseRet_t-1_t+1'] = price['close'].shift(-1) / price['close'].shift(1)-1
    price['CloseRet_t_t+1'] = price['close'].shift(-1) / price['close']-1
    price['CloseRet_t-1_t'] = price['close'] / price['close'].shift(1)-1
    return price

def get_label(df, indicators, thresholds):
    # 根据涨跌幅筛选有用新闻并设置标签
    def assign(df, indicator,threshold):
        df['label'] = np.nan
        df.loc[df[indicator]>threshold,'label'] = '1'
        df.loc[df[indicator]<1./(1+threshold)-1,'label'] = '0'
        return df[['date','label','title']]
    dfs = [assign(deepcopy(df),ind, thr) for ind,thr in zip(indicators,thresholds)]
    return dfs

def loadfiles(symbol):
    files = os.listdir('news2/%s/' % symbol)
    news = []
    for f in files:
        try:
            news_f = pd.read_csv('news2/%s/%s' % (symbol,f), dtype={'date':str})[['date','title','time','url']]
            news.append(news_f)
        except:
            continue
    if len(news) > 0:
        news = pd.concat(news)
        news['urlid'] = [','.join(url.split(',')[-2:]) for url in news['url']]
        news = news.drop_duplicates('urlid',keep='last').drop(['url','urlid'],axis=1)
        if len(news) > 0:
            return news
    return None

def process(symbol, indicators, thresholds, first=False):
    # 总的处理流程，调用get_price，get_label等功能函数
    news = loadfiles(symbol)
    if news is None:
        return
    # news = pd.read_csv('news/%s.csv' % symbol,dtype={'date':str})[['date','title','time']]

    news = news.dropna()
    news = news.sort_values('date')
    # 剔除无用消息
    nonsense=['融资融券信息','融资净偿还','融资净买入','融券净偿还','大宗交易','今日超大单流','龙虎榜','下跌','上涨','跌幅','涨幅','涨停','跌停',
              '大涨','大跌','跳水','盘中','融资余额','反弹','回调','火箭发射','投资者关系']
    is_useful = np.array([not any(bool(re.search(w,l)) for w in nonsense) for l in news['title'].values])
    news = news[is_useful]
    news['title'] = [s.replace(code_name.loc[symbol].values[0],'') for s in news['title']]
    news = news.loc[[len(s)>3 for s in news['title']]]
    # 对15:00:00以后的新闻把日期调整到下一个交易日    
    news['date'] = list(map(lambda x:x.replace('-',''), news['date'].values))
    news = news.merge(index[['date','next_date']],on='date',how='left')
    news.loc[news['time']>'13:00:00','date'] = np.nan
    news['date'] = news['date'].fillna(news['next_date'])
    try:
        price = get_price(symbol)
    except:
        return
    news = news.merge(price)
    dfs = get_label(news, indicators, thresholds)
    
    # 存储数据
    for ind,df in zip(indicators, dfs):
        df['code'] = symbol
        if not os.path.exists('%s/clean/' % output_dir):
            os.makedirs('%s/clean/' % output_dir)
        if first:
            df.to_csv('%s/clean/%s.csv' % (output_dir, ind), index=False)
        else:
            df.to_csv('%s/clean/%s.csv' % (output_dir, ind), mode='a', header=False,  index=False)

def split_train_test(filenames, sep_date, dev_size=0.1):
    
    for filename in filenames:
        print('shuffle and split %s' % filename)
        if not os.path.exists('%s/shuffle/%s' % (output_dir, filename)):
            os.makedirs('%s/shuffle/%s' % (output_dir, filename))

        df = pd.read_csv('%s/clean/%s.csv' % (output_dir, filename),dtype={'date':str,'label':str})
        idx_train = (df['date']<sep_date) & (~df['label'].isnull())
        idx_test = (df['date']>=sep_date) & (~df['label'].isnull())
        num_train = int(sum(idx_train) * (1-dev_size))
        # df = shuffle(df)
        # x_data, y_data = df['title'], df['label']
        # x_train, x_test, y_train, y_test = train_test_split(df['title'], df['label'], test_size=test_size, shuffle=True)
        
        train = shuffle(shuffle(shuffle(df.loc[idx_train][['label','title']].rename(columns={'title':'x_train'}), random_state=0),random_state=0),random_state=0)
        print(len(train[train['label']==1]) / len(train))
        train.iloc[:num_train].to_csv('%s/shuffle/%s/train.csv' % (output_dir, filename), index=False, sep='\t')
        train.iloc[num_train:].rename(columns={'x_train':'x_valid'}).to_csv('%s/shuffle/%s/dev.csv' % (output_dir, filename), index=False, sep='\t')
        
        test = pd.concat([shuffle(df.loc[idx_test]), df.loc[~idx_test]]).rename(columns={'title':'x_test'})
        test[['label','x_test']].to_csv('%s/shuffle/%s/test.csv' % (output_dir, filename), index=False, sep='\t')
        test[['date','code']].to_csv('%s/shuffle/%s/test_date.csv' % (output_dir, filename), index=False, sep='\t')

    
indicators = ['CloseRet_t-2_t+1','CloseRet_t-1_t+1','CloseRet_t_t+1']
thresholds = [0.1,0.08,0.05]
sep_date='20190101'
nf = newsfiles[0]
process(nf,indicators, thresholds, first=True)
for i,nf in enumerate(newsfiles[1:]):
    if (len(nf)==9):
        print('processing %s %d/%d' % (nf, i, len(newsfiles)))
        process(nf,indicators, thresholds, first=False)

split_train_test(indicators,sep_date)


