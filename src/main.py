import logging
from typing import List
from urllib.request import urlopen
from xml.etree import ElementTree

from mongo import MongoDBPipeline

logging.basicConfig(format="%(message)s", level=logging.INFO)
RSS_URL = "http://www.zone-h.org/rss/specialdefacements"

class ZoneHRss(object):
    def __init__(self):
        self.data: List[RssField] = list()

        tree = ElementTree.parse(urlopen(RSS_URL))
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
        self["archive"]     = False

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

    del mongo
