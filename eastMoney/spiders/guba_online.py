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

class GubaOnlineSpider(scrapy.Spider):
    name = 'guba_online'

    def __init__(self, symbol, **kwargs):
        self.allowed_domains = ['gb.eastmoney.com']
        self.base_url_prefix = 'http://gb.eastmoney.com'
        self.base_url = 'http://gb.eastmoney.com/list,%s,1,f.html' % symbol[:6]
        self.start_urls = [self.base_url]
        self.symbol = symbol
        # obtain the number of pages
        subpage_url = 'http://gb.eastmoney.com/list,%s,1,f_{}.html' % symbol[:6]
        pageresponse = requests.post(self.base_url)
        if not Selector(text=pageresponse.text).xpath('//div[@class="noarticle"]').extract_first() is None:
            numpage = 0
        else:
            pageresponse_text = Selector(text=pageresponse.text).xpath('//span[@class="pagernums"]').extract_first()
            numpage = math.ceil(int(pageresponse_text.split('|')[-3]) / int(pageresponse_text.split('|')[-2]))

        stockname = Selector(text=pageresponse.text).xpath('//*[@id="stockname"]/a/@href').extract_first()
        if not stockname is None:
            stockname = stockname.split(',')[-1].split('.')[0]
            logging.warning(stockname)
            if stockname != symbol[:6]:
                self.start_urls=[]
                return
        else:
            self.start_urls=[]
            return
        #self.start_urls=[]
        #return
        # obtain the records number and the last new's time
        engine = create_engine('mysql://wy:,.,.,l@10.24.224.249/webdata?charset=utf8')
        sql = 'select S_INFO_WINDCODE, URL from EastMoney where S_INFO_WINDCODE=\'' + symbol + '\''
        df_record = pd.read_sql(sql,engine)
        self.last_URL = max([int(url.split(',')[-1]) for url in df_record['URL']])
        #logging.warning(str(self.last_URL) + '\nokkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk!')
        
        record_num = len(df_record)
        crawled_pages = record_num // 80 # the web display the news 80 lines per page
        start_page = max(crawled_pages,1)

        for i in range(numpage-start_page+1,0,-1):
            self.start_urls.append(subpage_url.format(i))
        super().__init__(**kwargs)

    def parse(self, response):
        pass
        article_list = response.xpath('//div[@id="articlelistnew"]/div[@class="articleh normal_post"]')
        for article in article_list:
            item = EastmoneyItem()
            read_str = article.xpath('.//span[@class="l1 a1"]/text()').extract_first()
            item['symbol'] = self.symbol
            item['read'] = str((int(bool(re.search('万',read_str)))*9999+1)*float(read_str.replace('万','')))
            item['comment'] = article.xpath('.//span[@class="l2 a2"]/text()').extract_first()
            detail_url_part = article.xpath('.//span[@class="l3 a3"]/a/@href').extract_first()
            detail_url = self.base_url_prefix + detail_url_part
            url_id = int(detail_url_part[1:-5].split(',')[-1])
            item['url'] = detail_url_part[1:-5]
            if url_id <= self.last_URL:
                continue
            yield scrapy.Request(detail_url, callback=self.parse1, meta={'item':item})
            sleep(0.1)
        sleep(1)

    def parse1(self, response):
        item = response.meta['item']
        datetime = response.xpath('//div[@class="zwfbtime"]/text()').extract_first()
        item['date'] = datetime.split()[1].replace('-','')
        item['time'] = datetime.split()[2]
        item['title'] = response.xpath('//div[@id="zwconttbt"]/text()').extract_first().strip()
        item['content'] = " ".join(response.xpath('//div[@id="zwconbody"]//p/text()').extract())
        yield item

