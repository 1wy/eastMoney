# -*- coding: utf-8 -*-
import scrapy
from ..items import EastmoneyItem

class MoneySpider(scrapy.Spider):
    name = 'money'
    allowed_domains = ['finance.eastmoney.com']
    start_urls = []
    base_url = 'http://finance.eastmoney.com/news/cgsxw_{}.html'
    for i in range(1, 26):
        start_urls.append(base_url.format(i))

    def parse(self, response):
        article_list = response.xpath('//ul[@id="newsListContent"]/li')
        for article in article_list:
            detail_url = article.xpath('.//a/@href').extract_first()
            item = EastmoneyItem()
            abstract1 = article.xpath('.//p[@class="info"]/@title')
            abstract2 = article.xpath('.//p[@class="info"]/text()')
            item['abstract'] = abstract1.extract_first().strip() if len(abstract1) else abstract2.extract_first().strip()
            yield scrapy.Request(detail_url, callback=self.parse1, meta={'item':item})

    def parse1(self, response):
        item = response.meta['item']
        item['website'] = '东方财富网'
        item['url'] = response.url
        item['title'] = response.xpath('//div[@class="newsContent"]/h1/text()').extract_first()
        p_list = response.xpath('//div[@id="ContentBody"]/p')
        item['content'] = '\n'.join([p.xpath('.//text()').extract_first().strip() for p in p_list if len(p.xpath('.//text()'))]).strip()
        item['datetime'] = response.xpath('//div[@class="time-source"]/div[@class="time"]/text()').extract_first()
        item['original'] = response.xpath('//div[@class="source data-source"]/@data-source').extract_first()
        item['author'] = response.xpath('//p[@class="res-edit"]/text()').extract_first().strip()
        yield item


