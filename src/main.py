from xml.etree import ElementTree
from pymongo import MongoClient
from pymongo.results import InsertManyResult
from typing import Any, Dict, List
from urllib.request import urlopen
import logging

logging.basicConfig(format="%(message)s", level=logging.INFO)
RSS_URL = "http://www.zone-h.org/rss/specialdefacements"

class MongoDBPipeline(object):
    """
    MongoDBPipeline
    https://github.com/realpython/realpython-blog/blob/db5577c3f1fe6b32cfcd21e9a2583063f8b15039/2014/2014-12-31-web-scraping-with-scrapy-and-mongodb.markdown
    """
    def __init__(self, ip: str = '0.0.0.0', port: int = 27017):
        connection = MongoClient(ip, port)
        db = connection.scraping
        self.collection = db.links

    def reset(self):
        self.collection.delete_many({})

    def process_items(self, items: List[Dict[str, Any]]):
        res:InsertManyResult = self.collection.insert_many(items)
        if res:
            logging.info('Complete!')
        else:
            logging.warning('Failed to add items...')
            print(res)

        # remove Duplicate columns
        cursor = self.collection.aggregate(
            [
                {'$group': {"_id": "$title", "unique_ids": {"$addToSet": "$_id"}, "count": {"$sum": 1}}},
                {"$match": {"count": { "$gte": 2 }}}
            ]
        )
        for doc in cursor:
            del doc["unique_ids"][0]
            for id in doc["unique_ids"]:
                self.collection.delete_many({"_id": id})

    def show(self):
        for link in self.collection.find().sort('_id'):
            print(link)


class ZoneHRss(object):
    def __init__(self):
        self.data: List[RssField] = list()

        tree = ElementTree.parse(urlopen(RSS_URL))
        # tree = ElementTree.parse("specialfacements.rss")
        root = tree.getroot()

        self.data = [RssField(item) for item in root.findall('channel/item')]

    def printAll(self):
        for rssfield in self.data:
            rssfield.print()


class RssField(dict):
    def __init__(self, item: ElementTree.Element):
        super().__init__(self)
        self["title"] = item.find('title').text
        self["link"]  = item.find('link').text
        self["guid"]  = item.find('guid').text
        self["description"] = item.find('guid').text
        self["pubDate"]     = item.find('pubDate').text

    def print(self):
        print(self["title"], self["link"], self["guid"], self["description"], self["pubDate"])


if __name__ == '__main__':
    import argparse
    logging.info('zone-h RSS reader')

    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--reset", action='store_true')
    parser.add_argument("-i", "--ip", default='0.0.0.0')
    parser.add_argument("-p", "--port", default=27017)
    parser.add_argument("-s", "--show", action='store_true')
    args= parser.parse_args()

    mongo = MongoDBPipeline(args.ip, args.port)

    if args.reset:
        logging.info('rm database')
        mongo.reset()
    elif args.show:
        mongo.show()
    else:
        rss = ZoneHRss()
        mongo.process_items(rss.data)
