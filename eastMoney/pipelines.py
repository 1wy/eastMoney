# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
import pymysql
import logging
import pandas as pd
from sqlalchemy import create_engine
import datetime
class EastMoneyPipeline(object):
    def __init__(self):
        # 连接数据库
        # self.connect = pymysql.connect(
        #     host='10.24.224.249',  # 数据库地址
        #     port=3306,  # 数据库端口
        #     db='webdata',  # 数据库名
        #     user='wy',  # 数据库用户名
        #     passwd=',.,.,l',  # 数据库密码
        #     charset='utf8',  # 编码方式
        #     use_unicode=True)
        # # 通过cursor执行增删查改
        # self.cursor = self.connect.cursor()
        self.mysql_conn = create_engine('mysql://wy:,.,.,l@10.24.224.249/webdata?charset=utf8')
        self.inc_cnt = 0
        self.date = datetime.datetime.now().strftime('%Y-%m-%d')

    def process_item(self, item, spider):
        df = pd.DataFrame([[item['symbol'],item['trade_date'],item['date'],item['time'],item['title'],item['content'],item['comment'],item['read'],item['url']]],
        	columns=['S_INFO_WINDCODE', 'TRADE_DT','DATE', 'TIME', 'TITLE', 'CONTENT', 'COMMENTNUM', 'READNUM', 'URL'])
        #try:
        df.to_sql(name='EastMoney', con=self.mysql_conn, if_exists='append', index=False)
        self.inc_cnt += 1
        self.symbol = item['symbol']
        #except:
        #    pass
        # self.cursor.execute(
        #     """insert ignore into EastMoney(S_INFO_WINDCODE, DATE, TIME, TITLE, CONTENT, COMMENTNUM, READNUM)
        #     value (%s, %s, %s, %s, %s, %s, %s)""",  # 纯属python操作mysql知识，不熟悉请恶补
        #     (item['symbol'],  # item里面定义的字段和表字段对应
        #      item['date'],
        #      item['time'],
        #      item['title'],
        #      item['content'],
        #      item['comment'],
        #      item['read']))
        # 提交sql语句
        return item  # 必须实现返回
    
    def open_spider(self, spider):
        if not os.path.exists('log/update-%s.log' % self.date):
            with open('log/update-%s.log' % self.date,'w') as f:
                f.write('S_INFO_WINDCODE,RECORD_NUM,INC_NUM,NOW_NUM,TOT_NUM\n')

    def close_spider(self, spider):
        with open('log/update-%s.log' % self.date,'a') as f:
            f.write('%s,%d,%d,%d,%d\n' % (spider.symbol, spider.record_num,self.inc_cnt, spider.record_num+self.inc_cnt,spider.tot_msg_num))
