# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from pymongo import MongoClient


class BundestagProtocolCrawlerPipeline:
    def __init__(self):
        self.connection = MongoClient('mongodb://admin:pw@141.45.146.163:27017')
        database = self.connection['protocols']
        self.collection = database['19']

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        if self.collection.find(item).count() == 0:
            self.collection.insert_one(item)
        return item
