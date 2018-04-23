# -*- coding: utf-8 -*-
import scrapy
import time
from caijin.items import CaijinItem


class CaijinpostionSpider(scrapy.Spider):
    name = 'caijinPostion'
    allowed_domains = ['caijing.com.cn']
    start_urls = ['http://finance.caijing.com.cn/index.html']

    def parse(self, response):
        nodes = response.xpath("//div[@class='ls_news_c ls_news_r']/a")
        for n in nodes:
            post_url = n.xpath("@href").extract_first("")
            print 'url+++++++++++++++++++++:', post_url
            yield scrapy.Request(url=post_url, callback=self.pase_detail)

    def pase_detail(self, response):
        item = CaijinItem()
        item['title'] = response.xpath("//*[@id='cont_title']/text()").extract()[0]
        ntime = response.xpath("//*[@id='pubtime_baidu']/text()").extract()[0]

        # item['time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # item['time'] = datetime.datetime.strptime(ntime,"%Y-%m-%d %H:%M:%S").date()
        item['ntime'] = ntime
        source = response.xpath("//*[@id='source_baidu']/text()").extract()
        if(source):
            item['source'] = source[0]
        else:
            item['source'] = ''
        item['detail'] = response.xpath("//div[@class='article-content']").extract()[0]
        if(response.url):
            if(response.url.split('/')[4]):
                nurl = response.url.split('/')[4]
                item['nid'] = int(nurl.split('.')[0])
        else:
            item['nid'] = 0

        yield item

