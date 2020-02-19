# -*- coding: utf-8 -*-
import scrapy
import requests
from scrapy.selector import Selector
from ..items import EastmoneyItem
import math

class GubaSpider(scrapy.Spider):
    name = 'guba'

    def __init__(self, symbol, **kwargs):
        self.allowed_domains = ['guba.eastmoney.com']
        self.base_url_prefix = 'http://guba.eastmoney.com'
        self.base_url = 'http://guba.eastmoney.com/list,%s,1,f.html' % symbol
        self.start_urls = [self.base_url]

        # obtain the number of pages
        subpage_url = 'http://guba.eastmoney.com/list,%s,1,f_{}.html' % symbol
        pageresponse = requests.post(self.base_url)
        pageresponse = Selector(text=pageresponse.text).xpath('//span[@class="pagernums"]').extract_first()
        numpage = math.ceil(int(pageresponse.split('|')[-3]) / int(pageresponse.split('|')[-2]))

        # record the right number of news
        with open('log/web_%s.txt' % symbol, 'w') as f:
            f.write('%d' % int(pageresponse.split('|')[-3]))

        # add all the urls of pages
        for i in range(1,numpage+1):
            self.start_urls.append(subpage_url.format(i))
        super().__init__(**kwargs)

    def parse(self, response):
        pass
        article_list = response.xpath('//div[@id="articlelistnew"]/div[@class="articleh normal_post"]')
        for article in article_list:
            item = EastmoneyItem()
            item['read'] = article.xpath('.//span[@class="l1 a1"]/text()').extract_first()
            item['comment'] = article.xpath('.//span[@class="l2 a2"]/text()').extract_first()
            detail_url = self.base_url_prefix + article.xpath('.//span[@class="l3 a3"]/a/@href').extract_first()
            yield scrapy.Request(detail_url, callback=self.parse1, meta={'item':item})
        pass

    def parse1(self, response):
        item = response.meta['item']
        item['url'] = response.url
        datetime = response.xpath('//div[@class="zwfbtime"]/text()').extract_first()
        item['date'] = datetime.split()[1]
        item['time'] = datetime.split()[2]
        item['title'] = response.xpath('//div[@id="zwconttbt"]/text()').extract_first().strip()
        item['content'] = " ".join(response.xpath('//div[@id="zwconbody"]//p/text()').extract())
        # item['original'] = response.xpath('//div[@id="zw_header"]/span[@class="source"]/text()').extract_first()
        # print(item['date'], item['time'], item['url'])
        yield item

