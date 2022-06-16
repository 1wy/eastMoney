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

engine1 = create_engine("oracle://wind:wind@10.23.153.15:21010/wind")


# ### 下载指数价格序列

idxs = ['000300.SH','000905.SH','000985.CSI']
idxs_name = dict(zip(idxs,['沪深300','中证500','中证全指']))
#updb = UpdateDB('10.24.224.249','fineng','123456')
#IdxPrice = updb.pull_index(idxs).loc['20100101':].reset_index().rename(columns={'TRADE_DT':'date'}).sort_values('date')
#IdxPrice = updb.pull_index(idxs).loc['20100101':].reset_index().rename(columns={'TRADE_DT':'date'}).sort_values('date')
IdxPrice = pd.read_sql('select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AIndexEODPrices where S_INFO_WINDCODE in (\'%s\')' % "\',\'".join(['000905.SH','000300.SH']),engine1).pivot_table(index='trade_dt',columns='s_info_windcode',values='s_dq_close').loc['20100101':].reset_index().rename(columns={'trade_dt':'date'}).sort_values('date')
IdxPrice['date'] = [d[:4]+'-'+d[4:6]+'-'+d[6:] for d in IdxPrice['date']]
IdxPrice = IdxPrice.set_index('date')

WeightTables = ['AIndexHS300CloseWeight','AIndexCSI500Weight']
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

def compute_riskappe(Stock, component):
    if not (component is None):
        Stock = component.merge(Stock,on=['code','date'],how='left')
    print(Stock)
    RiskAppe = Stock.groupby('date')[['RET-20','STD-20']].corr().iloc[::2,1].droplevel(1).reset_index().rename(columns={'STD-20':'RiskAppe'}).set_index('date')
    return RiskAppe

MarketSent = {}
for idx in idxs:
    RiskAppe = compute_riskappe(deepcopy(Stock), WTDict[idx])
    MarketSent[idx] = RiskAppe
MarketSent['000985.CSI']

with pd.ExcelWriter('alyData/风险偏好跟踪%s.xlsx' % enddate) as writer:
    idx = '000985.CSI'
#     for idx in idxs:
    MarketSent[idx].to_excel(writer,idxs_name[idx])

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

