# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import codecs
import pymysql
import time
from twisted.enterprise import adbapi

class CaijinPipeline(object):
    def process_item(self, item, spider):
        return item

class JsonWithEncodingPipeline(object):
    def __init__(self):
        # self.filename = open("caijin.json", "w")
        self.file = codecs.open("caijin.json", 'w', encoding="utf-8")

    def process_item(self, item, spider):
        lines = json.dumps(dict(item), ensure_ascii=False) + ",\n"
        self.file.write(lines)
        return item

    def close_spider(self, spider):
        self.filename.close()

class MysqlPipeline(object):
    '''
    插入MySQL数据库
    '''

    def __init__(self):
        self.conn = pymysql.connect('127.0.0.1', 3306, user='root', passwod='', db='caijing', user_unicode=True, charset='utf8')
        self.cursor = self.conn.cursor()

    def process_item(self,item,spider):
        insert_sql = '''
        insert into zt_caijing(nid,title,source,detail,n_time,c_time) VALUES (%s,%s,%s,%s,%s,%s)
        '''

        self.cursor.execute(insert_sql, (item['nid'], item['title'], item['source'], item['detail'], item['ntime'], time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        self.conn.commit()

class MysqlTwistedPipline(object):
    '''
        采用异步的方式插入数据
    '''

    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls,settings):
        dbparms = dict(
            host=settings["MYSQL_HOST"],
            port=settings["MYSQL_PORT"],
            user=settings["MYSQL_USER"],
            passwd=settings["MYSQL_PASSWD"],
            db=settings["MYSQL_DB"],
            use_unicode=True,
            charset="utf8",

        )
        dbpool = adbapi.ConnectionPool('pymysql', **dbparms)
        return cls(dbpool)

    def process_item(self, item, spider):
        '''
        使用twisted将mysql插入变成异步
        :param item:
        :param spider:
        :return:
        '''
        query = self.dbpool.runInteraction(self.do_insert,item)
        query.addErrback(self.handle_error)

    def handle_error(self, failure):
        #处理异步插入异常
        print(failure)

    def do_insert(self, cursor, item):
        # 查重处理
        cursor.execute(
            """select nid from zt_caijing where nid = %s""",
            item['nid'])
        # 是否有重复数据
        repetition = cursor.fetchone()
        if repetition:
            pass

        else:
            # 具体插入数据
            insert_sql = '''
                    insert into zt_caijing(nid,title,source,detail,n_time,c_time) VALUES (%s,%s,%s,%s,%s,%s)
                    '''
            cursor.execute(insert_sql, (item['nid'], item['title'], item['source'], item['detail'], item['ntime'],time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))












