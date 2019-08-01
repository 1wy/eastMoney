#!/usr/bin/env python
# coding: utf-8

# study the impact of the news
import os
import re
import thulac
import pandas as pd
import numpy as np
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# load news data of 002230.SZ
price_all = pd.read_csv('../data/wind/ashareeodprices.csv')

index_all = pd.read_csv('../data/wind/aindexeodprices.csv')
index = index_all[index_all['S_INFO_WINDCODE'] == '000906.SH'].loc[:, ['TRADE_DT', 'S_DQ_CLOSE']].sort_values('TRADE_DT')
index.columns = ['date', 'idxclose']
index['idxpast10'] = index['idxclose'] / index['idxclose'].shift(10).values - 1
index['idxpast5'] = index['idxclose'] / index['idxclose'].shift(5).values - 1
index['idxfuture5'] = index['idxclose'].shift(-5).values / index['idxclose'] - 1
index['idxfuture10'] = index['idxclose'].shift(-10).values / index['idxclose'] - 1

newsfiles = os.listdir('news')
for i,nf in enumerate(newsfiles):

    if (len(nf)==13):
        symbol = nf[:9]
        print('processing %s %d/%d' % (symbol, i, len(newsfiles)))
        news = pd.read_csv('news/%s.csv' % symbol)[['date','title']]
        news = news.dropna()
        news = news.sort_values('date')
        news['title'] = news['title'].astype(str)
        is_useful = np.array([not bool(re.search('融资融券信息',l)) for l in news['title'].values])
        is_useful = is_useful & np.array([not bool(re.search('大宗交易数据',l)) for l in news['title'].values])
        is_useful = is_useful & np.array([not bool(re.search('今日超大单流',l)) for l in news['title'].values])
        is_useful = is_useful & np.array([not bool(re.search('龙虎榜',l)) for l in news['title'].values])
        news = news[is_useful]
        news['date'] = list(map(lambda x: int(x.replace('-','')), news['date'].values))
        news['title'] = [l+' ' for l in news['title'].values]
        news = news.groupby('date').sum().reset_index()
        news['title'] = [re.sub('\d+|[a-z]+|[A-Z]+', '0',s) for s in news['title'].values]

        # clean and segment the news
        punctuation = '"#$&\'()*+,-/:;<=>@[\\]^_`{|}~.!?＂＃＄＆＇（）＊＋，－／：；＜＝＞＠［＼］＾＿｀｛｜｝～｟｠｢｣〃〈〉《》「」『』【】〔〕〖〗〘〙〚〛〜〝〞〟〰〾〿–—‘’‛“”„‟…‧﹏﹑﹔·！？｡。'
        thu1 = thulac.thulac(user_dict='ipy/company_name.csv', seg_only=True)
        news['title'] = [s.translate(str.maketrans('','',punctuation)) for s in news['title'].values]
        news['title'] = [thu1.cut(s, text=True) for s in news['title'].values]

        # load price data


        price = price_all[price_all['S_INFO_WINDCODE']==symbol]
        price = price.sort_values('TRADE_DT').loc[:,['TRADE_DT','S_DQ_ADJCLOSE']]
        price.columns = ['date', 'close']
        price['past10'] = price['close'] / price['close'].shift(10).values - 1
        price['past5'] = price['close'] / price['close'].shift(5).values - 1
        price['future5'] = price['close'].shift(-5).values / price['close'] - 1
        price['future10'] = price['close'].shift(-10).values / price['close'] - 1

        df_p = price.merge(index,on=['date'])
        df_p['expast10'] = df_p['past10'] - df_p['idxpast10']
        df_p['expast5'] = df_p['past5'] - df_p['idxpast5']
        df_p['exfuture5'] = df_p['future5'] - df_p['idxfuture5']
        df_p['exfuture10'] = df_p['future10'] - df_p['idxfuture10']

        # merger news data and price data
        df = news[['date','title']].merge(df_p[['date','expast10','expast5','exfuture5','exfuture10']],on=['date'])
        # df.style.bar(subset=['expast10','expast5','exfuture5','exfuture10'],align='zero',color=['#5fba7d','#d65f5f'])
        # df.sort_values('exfuture5',ascending=False).style.bar(subset=['expast10','expast5','exfuture5','exfuture10'],align='zero',color=['#5fba7d','#d65f5f'])
        df.to_csv('data/%s.csv' % symbol,quoting=1)


        pos_text = df[df['exfuture5']>0.01]['title'].sum()
        neg_text = df[df['exfuture5']<-0.01]['title'].sum()

        if (sum(df['exfuture5']>0.01) > 1) and (sum(df['exfuture5']<-0.01) > 1) and (len(pos_text.strip()) > 1) and(len(neg_text.strip()) > 1):
            pos_keys = list(set(pos_text.split()))
            posdict = dict(zip(pos_keys, np.zeros(len(pos_keys),dtype=int)))
            for w in pos_text.split():
                posdict[w] += 1
            posdict = pd.DataFrame({'word':list(posdict.keys()), 'freq':list(posdict.values())})
            posdict = posdict.sort_values('freq',ascending=False)

            neg_keys = list(set(neg_text.split()))
            negdict = dict(zip(neg_keys, np.zeros(len(neg_keys),dtype=int)))
            for w in neg_text.split():
                negdict[w] += 1
            negdict = pd.DataFrame({'word':list(negdict.keys()), 'freq':list(negdict.values())})
            negdict = negdict.sort_values('freq',ascending=False)

            stopwords= list(set(posdict.iloc[:30]['word'].values) & set(negdict.iloc[:30]['word'].values))
            if (len(set(pos_keys)-set(stopwords)) > 0) and (len(set(neg_keys)-set(stopwords)) > 0):
                pos_text = df[df['exfuture5']>0.01]['title'].sum()
                wc = WordCloud(max_words=800,stopwords=stopwords,font_path='ipy/SimSun.ttf',width=1400, height=700, margin=2).generate(pos_text)
                plt.figure(1, figsize=(20,10))
                wc.to_file('figures/%s_pos.png' % symbol)

                neg_text = df[df['exfuture5']<-0.01]['title'].sum()
                wc = WordCloud(max_words=800,stopwords=stopwords,font_path='ipy/SimSun.ttf',width=1400, height=700, margin=2).generate(neg_text)
                plt.figure(1, figsize=(20,10))
                wc.to_file('figures/%s_neg.png' % symbol)



