# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field

class EastmoneyItem(scrapy.Item):
    url = Field()
    title = Field()
    content = Field()
    date = Field()
    time = Field()
    read = Field()
    comment = Field()
