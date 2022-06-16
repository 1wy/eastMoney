#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
from updatedb import UpdateDB
from datetime import datetime
from copy import deepcopy
from sqlalchemy import create_engine

# ## 读取情绪分数数据
startdate = '2012-01-01'
enddate = datetime.now().strftime('%Y-%m-%d')
#enddate = '2021-01-21'

engine1 = create_engine("mysql+pymysql://wy:,.,.,l@localhost/wind")
Sent1 = pd.read_sql('select S_INFO_WINDCODE, TRADE_DT, SCORE, TITLE from FinancialNews where (SCORE is not null) and (USEFUL=1)', engine1).rename(columns={'S_INFO_WINDCODE':'code','TRADE_DT':'date','SCORE':'prob'}).sort_values('date')

engine2 = create_engine("mysql+pymysql://wy:,.,.,l@localhost/webdata")
Sent2 = pd.read_sql('select S_INFO_WINDCODE, TRADE_DT, SCORE, TITLE from EastMoney where (SCORE is not null) and (USEFUL=1)', engine2).rename(columns={'S_INFO_WINDCODE':'code','TRADE_DT':'date','SCORE':'prob'}).sort_values('date')

Sent = pd.concat([Sent1,Sent2])
Sent['date'] = [d[:4]+'-'+d[4:6]+'-'+d[6:] for d in Sent['date']]
Sent['prob'] = (Sent['prob']-0.5)*2

# ### 下载指数价格序列

idxs = ['000300.SH','000905.SH','000985.CSI']
idxs_name = dict(zip(idxs,['沪深300','中证500','中证全指']))
updb = UpdateDB('10.24.224.249','fineng','123456')
IdxPrice = updb.pull_index(idxs).loc['20100101':].reset_index().rename(columns={'TRADE_DT':'date'}).sort_values('date')
IdxPrice['date'] = [d[:4]+'-'+d[4:6]+'-'+d[6:] for d in IdxPrice['date']]
IdxPrice = IdxPrice.set_index('date')

WeightTables = ['MyAIndexHS300CloseWeight','MyAIndexCSI500Weight']
WTDict = {}
for idx,wt in zip(idxs[:-1],WeightTables):
    WTDict[idx] = pd.read_sql('select * from %s where TRADE_DT>\'20091231\'' % wt, engine1).rename(columns={'S_CON_WINDCODE':'code','TRADE_DT':'date'}).sort_values('date')
    WTDict[idx]['date'] = [d[:4]+'-'+d[4:6]+'-'+d[6:] for d in WTDict[idx]['date']]
WTDict['000985.CSI'] = None

# ### 下载股票数据

Stock = pd.read_sql('select S_INFO_WINDCODE, TRADE_DT, S_DQ_ADJCLOSE from MyAShareEODPrices where TRADE_DT>20100101', engine1).rename(columns={'S_INFO_WINDCODE':'code','TRADE_DT':'date'}).sort_values('date')
Stock['date'] = [d[:4]+'-'+d[4:6]+'-'+d[6:] for d in Stock['date']]
Stock['RET'] = Stock['S_DQ_ADJCLOSE'] / Stock.groupby('code')['S_DQ_ADJCLOSE'].shift(1)-1
Stock['logRET'] = np.log(Stock['RET'].add(1))
Stock_GB = Stock.groupby('code')
Stock['RET-20'] = Stock_GB['logRET'].rolling(20).sum().droplevel(0)
Stock['RET-10'] = Stock_GB['logRET'].rolling(10).sum().droplevel(0)
Stock['RET-5'] = Stock_GB['logRET'].rolling(5).sum().droplevel(0)
Stock['RET-3'] = Stock_GB['logRET'].rolling(3).sum().droplevel(0)
Stock['STD-20'] = Stock_GB['logRET'].rolling(20).std().droplevel(0)
Stock['STD-60'] = Stock_GB['logRET'].rolling(60).std().droplevel(0)
Stock['STD-120'] = Stock_GB['logRET'].rolling(120).std().droplevel(0)
Stock['STD-10'] = Stock_GB['logRET'].rolling(10).std().droplevel(0)
Stock['STD-5'] = Stock_GB['logRET'].rolling(5).std().droplevel(0)
Stock['RET1'] = Stock_GB['logRET'].shift(-1)
Stock['RET3'] = Stock_GB['logRET'].rolling(3).sum().droplevel(0).shift(-3)
Stock['RET5'] = Stock_GB['logRET'].rolling(5).sum().droplevel(0).shift(-5)
Stock


# ## 数据综合&标签计算
df_sent = Sent.merge(Stock,on=['code','date'],how='left')

# ## 计算全市场情绪指标
# 计算指数的衍生指标
def compute_rets(df,col):
    df['RET-20'] = 100*(df.loc[:,col] / df.loc[:,col].shift(20)-1)
    df['RET-5'] = 100*(df.loc[:,col] / df.loc[:,col].shift(5)-1)
    df['RET-3'] = 100*(df.loc[:,col] / df.loc[:,col].shift(3)-1)
    df['RET'] = 100*(df.loc[:,col] / df.loc[:,col].shift(1)-1)
    df['RET1'] = 100*(df.loc[:,col].shift(-1) / df.loc[:,col]-1)
    df['RET3'] = 100*(df.loc[:,col].shift(-3) / df.loc[:,col]-1)
    df['RET5'] = 100*(df.loc[:,col].shift(-5) / df.loc[:,col]-1)
    df['RET10'] = 100*(df.loc[:,col].shift(-10) / df.loc[:,col]-1)
    df['RET20'] = 100*(df.loc[:,col].shift(-20) / df.loc[:,col]-1)
    df['RET3-5'] = 100*(df.loc[:,col].shift(-5) / df.loc[:,col].shift(-3)-1)
    df['RET5-10'] = 100*(df.loc[:,col].shift(-10) / df.loc[:,col].shift(-5)-1)
    df['RET10-20'] = 100*(df.loc[:,col].shift(-20) / df.loc[:,col].shift(-10)-1)
    df['RETSTD-3'] = df['RET'].rolling(3).std()
    df['RETSTD-5'] = df['RET'].rolling(5).std()
    df['RETSTD-20'] = df['RET'].rolling(20).std()
    df['RETSTD-60'] = df['RET'].rolling(60).std()
    df['RETSTD3'] = df['RETSTD-3'].shift(-3)
    df['RETSTD5'] = df['RETSTD-5'].shift(-5)
    df['RETSTD3_change'] = df['RETSTD3']-df['RETSTD-3']
    df['RETSTD5_change'] = df['RETSTD5']-df['RETSTD-5']
    return df.reset_index().rename(columns={'index':'date'})

# 计算情绪指标的衍生指标
def compute_sent_derivative(df_sent):
    df_sent['sentisnan'] = df_sent['SENT'].isnull()
    df_sent.iloc[0]['SENT'] = 0
    df_sent['SENT'] = df_sent['SENT'].fillna(0)
    
    df_sent['MA5'] = df_sent['SENT'].rolling(5,min_periods=5).mean()
    df_sent['MA10'] = df_sent['SENT'].rolling(10,min_periods=10).mean()
    df_sent['MA20'] = df_sent['SENT'].rolling(20,min_periods=20).mean()
    df_sent['MA40'] = df_sent['SENT'].rolling(40,min_periods=20).mean()
    df_sent['MA60'] = df_sent['SENT'].rolling(60,min_periods=20).mean()

    df_sent['MA120'] = df_sent['SENT'].rolling(120,min_periods=120).mean()
    df_sent['MA240'] = df_sent['SENT'].rolling(240,min_periods=120).mean()
    
    df_sent['DIFF5'] = df_sent['SENT'] - df_sent['MA5']
    df_sent['DIFF10'] = df_sent['SENT'] - df_sent['MA10']
    df_sent['DIFF20'] = df_sent['SENT'] - df_sent['MA20']
    df_sent['DIFF5ABS'] = df_sent['DIFF5'].abs()
    df_sent['DIFF10ABS'] = df_sent['DIFF10'].abs()
    df_sent['DIFF20ABS'] = df_sent['DIFF20'].abs()
    df_sent['MA20-MA120'] = df_sent['MA20'] - df_sent['MA120']
    df_sent['DIFFM'] = df_sent['MA20-MA120'] - df_sent['MA20-MA120'].shift(20)
    df_sent['MA40-MA120'] = df_sent['MA40'] - df_sent['MA120']
    df_sent['MA60-MA120'] = df_sent['MA60'] - df_sent['MA120']

    df_sent['MA20-MA240'] = df_sent['MA20'] - df_sent['MA240']
    df_sent['MA10-MA120'] = df_sent['MA10'] - df_sent['MA120']
    df_sent['MA5-MA120'] = df_sent['MA5'] - df_sent['MA120']
    
    df_sent['STDMA5'] = df_sent['SENTSTD'].rolling(5,min_periods=5).mean()
    df_sent['STDDIFF5'] = df_sent['SENTSTD'] - df_sent['STDMA5']
    
    df_sent['SUMMA5'] = df_sent['SENTSUM'].rolling(5,min_periods=5).mean()
    df_sent['SUMMA120'] = df_sent['SENT'].rolling(120,min_periods=120).mean()
    df_sent['SUMDIFF5'] = df_sent['SENTSUM'] - df_sent['SUMMA5']
    
    df_sent['SEQSTD5'] = df_sent['SENT'].rolling(5,min_periods=5).std()
    
    return df_sent

def compute_sentidx(sent, component, index):
    if not (component is None):
        sent = component.merge(sent,on=['code','date'],how='left')
    sent_date_code = sent.groupby(['date','code'])['prob'].mean()
    avg_sent = sent_date_code.reset_index().groupby('date')['prob'].mean().reset_index().rename(columns={'prob':'SENT'})
    sum_sent = sent_date_code.reset_index().groupby('date')['prob'].sum().reset_index().rename(columns={'prob':'SENTSUM'})
    std_sent = sent_date_code.reset_index().groupby('date')['prob'].std().reset_index().rename(columns={'prob':'SENTSTD'})
    
    df_sent = index.merge(avg_sent,how='left').merge(sum_sent,how='left').merge(std_sent,how='left')
    df_sent = compute_sent_derivative(df_sent)
    return df_sent

def compute_riskappe(Stock, component):
    if not (component is None):
        Stock = component.merge(Stock,on=['code','date'],how='left')
    print(Stock)
    RiskAppe = Stock.groupby('date')[['RET-20','STD-20']].corr().iloc[::2,1].droplevel(1).reset_index().rename(columns={'STD-20':'RiskAppe'}).set_index('date')
    return RiskAppe

MarketSent = {}
for idx in idxs:
    idx_price = compute_rets(deepcopy(IdxPrice[[idx]]),idx)
    idx_sent = compute_sentidx(deepcopy(Sent), WTDict[idx], idx_price).set_index('date')
    RiskAppe = compute_riskappe(deepcopy(Stock), WTDict[idx])
    MarketSent[idx] = idx_sent.join(RiskAppe)
MarketSent['000985.CSI']


# ### 警示牛转熊
def exe_test_danger(thv,hotthv,df):
    df['danger'] = df['DIFFM']<thv
    df['hot'] = df['RET-20']>hotthv
    df['警示信号'] = np.nan
    
    useful_time = []
    signal = df.loc[df['hot'] & df['danger'],:]#.dropna()
    if len(signal) == 0:
        return df
    useful_time = [signal.index[0]]
    for i in range(1,len(signal)):
        if (datetime.strptime(signal.index[i],'%Y-%m-%d')-datetime.strptime(useful_time[-1],'%Y-%m-%d')).days>20:
            useful_time.append(signal.index[i])
    df.loc[useful_time,'警示信号'] = 1
    return df

def test_exhot(df):
    df_res = []
    thvs = np.arange(-0.07,0,0.01)
    hotthvs = np.arange(10,4,-1)
    df_res = pd.DataFrame(0,columns=hotthvs,index=thvs)
    for thv in thvs:
        for hotthv in hotthvs:
            signal = deepcopy(exe_test_danger(thv,hotthv,df))
            signal = signal[(~signal['警示信号'].isnull()) & (~signal['RET20'].isnull())]
            if len(signal)==0:
                df_res.loc[thv,hotthv] = 'NaN(0)'
            else:
                acc = sum(signal['RET20']<0)/len(signal)
                df_res.loc[thv,hotthv] = '%.2f(%d)' % (acc*100,len(signal))

    return df_res

def exe_test_chance(thv,hotthv,df):
    df['chance'] = df['DIFFM']>thv
    df['cool'] = df['RET-20']<hotthv
    df['反弹信号'] = np.nan

    useful_time = []
    signal = df.loc[df['chance'] & df['cool'],:]#.dropna()
    if len(signal) == 0:
        return df
    useful_time = [signal.index[0]]
    for i in range(1,len(signal)):
        if (datetime.strptime(signal.index[i],'%Y-%m-%d')-datetime.strptime(useful_time[-1],'%Y-%m-%d')).days>20:
            useful_time.append(signal.index[i])
    df.loc[useful_time,'反弹信号'] = 1
    return df

def test_chance(df):
    df_res = []
    thvs = np.arange(0,0.06,0.01)
    hotthvs = np.arange(-10,0,1)
    df_res = pd.DataFrame(0,columns=hotthvs,index=thvs)
    for thv in thvs:
        for hotthv in hotthvs:
            signal = deepcopy(exe_test_chance(thv,hotthv,df))
            signal = signal[(~signal['反弹信号'].isnull()) & (~signal['RET20'].isnull())]
            if len(signal) == 0:
                df_res.loc[thv,hotthv] = 'NaN(0)'
            else:
                acc = sum(signal['RET20']>0)/len(signal)
                df_res.loc[thv,hotthv] = '%d(%d)' % (round(acc*100),len(signal))
    
    return df_res

def process_before_save(df,idx,idxs_name):
    df = df.loc[startdate:,[idx,'MA20-MA120','DIFFM','RET-20','RiskAppe','警示信号','反弹信号']].rename(columns={idx:idxs_name[idx],'MA20-MA120':'情绪指标20日均线（调整）','DIFFM':'20日情绪变动','RET-20':'20日动量（%）','RiskAppe':'交易层面风险偏好'})
    df.insert(0,'日期',[d[:7] for d in df.index])
    df['警示信号'] = df['警示信号']*6000
    df['反弹信号'] = df['反弹信号']*6000
    return df

with pd.ExcelWriter('alyData/情绪指标跟踪%s.xlsx' % enddate) as writer:
    idx = '000985.CSI'
#     for idx in idxs:
    df = exe_test_danger(-0.03,7,deepcopy(MarketSent[idx]))
    df = exe_test_chance(0.03,-5,df)
    df = process_before_save(df,idx,idxs_name)
    df.to_excel(writer,idxs_name[idx])
    df.loc['2021-01-01':].to_excel(writer,idxs_name[idx]+'今年以来')    

# HS300 = deepcopy(MarketSent['000300.SH'])
# HS300_tra = deepcopy(HS300.loc[:'2018-12-31',:])
# HS300_tes = deepcopy(HS300.loc['2018-12-31':enddate,:])

# df_res_tra = test_exhot(HS300_tra)
# df_res_tes = test_exhot(HS300_tes)


# ZZ500 = deepcopy(MarketSent['000905.SH'])
# ZZ500_tra = deepcopy(ZZ500.loc[:'2018-12-31',:])
# ZZ500_tes = deepcopy(ZZ500.loc['2018-12-31':enddate,:])

# df_res_tra = test_exhot(ZZ500_tra)
# df_res_tes = test_exhot(ZZ500_tes)
# df_res_tra

