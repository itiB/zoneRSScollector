from pymongo import MongoClient
from pymongo.cursor import Cursor
from pymongo.results import InsertManyResult
from typing import Any, Dict, Iterator, List
import logging

class MongoDBPipeline(object):
    """
    MongoDBPipeline
    https://github.com/realpython/realpython-blog/blob/db5577c3f1fe6b32cfcd21e9a2583063f8b15039/2014/2014-12-31-web-scraping-with-scrapy-and-mongodb.markdown
    """
    def __init__(self, ip: str = '0.0.0.0', port: int = 27017):
        self.connection = MongoClient(ip, port)
        db = self.connection.scraping
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
    
    def get_non_archived_title(self) -> Iterator[str]:
        print('hoge')
        non_archived_list: Cursor = self.collection.find(filter={'archive': False})
        for non_archive in non_archived_list:
            yield non_archive['title']

    def get_archived_title(self) -> Iterator[str]:
        """
        ちょっとまちがえてたのを修正するための一時的なやつ
        """

        print('hoge')
        non_archived_list: Cursor = self.collection.find()
        for non_archive in non_archived_list:
            yield non_archive['title']

    def set_val(self, title: str, target: str, val: Any):
        update = self.collection.update_one(
            {'title': title},{'$set':{target: val}}
        )
        print(update)


    def show(self):
        for link in self.collection.find().sort('_id'):
            print(link)

    def __del__(self):
        self.connection.close()
