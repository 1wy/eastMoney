# -*- coding: utf-8 -*-
import math
import os
import re
import scrapy
import requests
import logging
from scrapy.selector import Selector
from ..items import EastmoneyItem
from time import sleep
import pandas as pd
from sqlalchemy import create_engine
import datetime 
import random
from time import time

class GubaOnlineSpider(scrapy.Spider):
    name = 'guba_online'

    def __init__(self, symbol, **kwargs):
        self.allowed_domains = ['gb.eastmoney.com']
        self.base_url_prefix = 'http://gb.eastmoney.com'
        self.base_url = 'http://gb.eastmoney.com/list,%s,1,f.html' % symbol[:6]
        self.start_urls = [self.base_url]
        self.symbol = symbol
        t1 = time()
        # obtain the number of pages
        subpage_url = 'http://gb.eastmoney.com/list,%s,1,f_{}.html' % symbol[:6]
        pageresponse = requests.post(self.base_url)
        self.tot_msg_num = 0
        self.num_per_page = 80
        if not Selector(text=pageresponse.text).xpath('//div[@class="noarticle"]').extract_first() is None:
            numpage = 0
        else:
            pageresponse_text = Selector(text=pageresponse.text).xpath('//span[@class="pagernums"]').extract_first()
            self.tot_msg_num = int(pageresponse_text.split('|')[-3])
            self.num_per_page = int(pageresponse_text.split('|')[-2])
            numpage = math.ceil(self.tot_msg_num / self.num_per_page)

        stockname = Selector(text=pageresponse.text).xpath('//*[@id="stockname"]/a/@href').extract_first()
        if not stockname is None:
            stockname = stockname.split(',')[-1].split('.')[0]
            logging.warning(stockname)
            if stockname != symbol[:6]:
                raise 
        else:
            self.start_urls=[]
            return
        t2 = time()
        # obtain the records number and the last new's time
        mysql_conn1 = create_engine('mysql://wy:,.,.,l@10.24.224.249/webdata?charset=utf8')
        sql = 'select S_INFO_WINDCODE, URL from EastMoney where S_INFO_WINDCODE=\'' + symbol + '\''
        df_record = pd.read_sql(sql,mysql_conn1)
        if len(df_record) ==0:
            self.last_URL = -1
        else:
            self.last_URL = max([int(url.split(',')[-1]) for url in df_record['URL']])
        t3 = time()
        # set proxy to avoid forbiddance 

        proxys = pd.read_sql('select ip from Proxy where score>0', mysql_conn1)['ip'].values
        sel_proxy = random.choice(proxys)
        if sel_proxy[:3] == '127':
            self.proxy = None
        else:
            self.proxy = 'https://wangxwang:898990@%s' % sel_proxy

        t4 = time()
        #logging.warning(str(self.last_URL) + '\nokkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk!')
        today = datetime.datetime.now()
        date_begin = (today + datetime.timedelta(days=-90)).strftime('%Y%m%d')
        date_end = (today + datetime.timedelta(days=30)).strftime('%Y%m%d')
        mysql_conn2 = create_engine('mysql://wy:,.,.,l@10.24.224.249/wind?charset=utf8')
        trade_days = pd.read_sql('select TRADE_DAYS from MyAShareCalendar where S_INFO_EXCHMARKET="SSE" order by TRADE_DAYS',mysql_conn2).rename(columns={'TRADE_DAYS':'TRADE_DT'})
        trade_days['date'] = trade_days['TRADE_DT']
        self.all_date = pd.DataFrame({'date':[str(d)[:10].replace('-','') for d in pd.date_range(date_begin,date_end)]})
        self.all_date = self.all_date.merge(trade_days[['date','TRADE_DT']],how='left')
        self.all_date['TRADE_DT'] = self.all_date['TRADE_DT'].bfill()
        # self.all_date['next_date'] = self.all_date['TRADE_DT'].shift(-1)
        self.all_date['next_date'] = self.all_date['date'].shift(-1)
        self.all_date = self.all_date.set_index('date')

        self.record_num = len(df_record)
        crawled_pages = self.record_num // self.num_per_page # the web display the news 80 lines per page
        start_page = max(crawled_pages,1)

        t5 = time()
        # print(t2-t1,t3-t2,t4-t3,t5-t4)
        for i in range(numpage-start_page+1,0,-1):
            self.start_urls.append(subpage_url.format(i))

        super().__init__(**kwargs)

    def parse(self, response):
        logging.warning(response.url)
        logging.warning(response.meta['download_latency'])
        article_list = response.xpath('//div[@id="articlelistnew"]/div[@class="articleh normal_post"]')
        for article in article_list:
            item = EastmoneyItem()
            read_str = article.xpath('.//span[@class="l1 a1"]/text()').extract_first()
            item['symbol'] = self.symbol
            read_str = article.xpath('.//span[@class="l1 a1"]/text()').extract_first()
            item['symbol'] = self.symbol
            item = EastmoneyItem()
            read_str = article.xpath('.//span[@class="l1 a1"]/text()').extract_first()
            item['symbol'] = self.symbol
            item['read'] = str((int(bool(re.search('万',read_str)))*9999+1)*float(read_str.replace('万','')))
            item['comment'] = article.xpath('.//span[@class="l2 a2"]/text()').extract_first()
            detail_url_part = article.xpath('.//span[@class="l3 a3"]/a/@href').extract_first()
            detail_url = self.base_url_prefix + detail_url_part
            url_id = int(detail_url_part[1:-5].split(',')[-1])
            item['url'] = detail_url_part[1:-5]
            if url_id > self.last_URL:
                yield scrapy.Request(detail_url, callback=self.parse1, meta={'proxy':self.proxy,'item':item,'download_timeout': 10.0})
            #sleep(0.01)
        sleep(1)

    def parse1(self, response):
        logging.warning(response.url)
        logging.warning(response.meta['download_latency'])
        
        item = response.meta['item']
        datetime = response.xpath('//div[@class="zwfbtime"]/text()').extract_first()
        item['date'] = datetime.split()[1].replace('-','')
        item['time'] = datetime.split()[2]
        item['trade_date'] = self.all_date.loc[item['date'],'next_date'] if (item['time']>'15:00:00') else item['date']
        item['trade_date'] = self.all_date.loc[item['trade_date'],'TRADE_DT']
        item['title'] = response.xpath('//div[@id="zwconttbt"]/text()').extract_first().strip()
        item['content'] = " ".join(response.xpath('//div[@id="zwconbody"]//p/text()').extract())
        yield item
