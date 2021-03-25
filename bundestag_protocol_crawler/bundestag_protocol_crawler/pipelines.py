# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from itemadapter import ItemAdapter
import pymongo


class BundestagProtocolCrawlerPipeline:
    def __init__(self):
        self.connection = pymongo.MongoClient('mongodb://localhost:27017')
        self.database = self.connection['bundestag']

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        self.database['protocols'].insert_one(ItemAdapter(item).asdict())
        return item
