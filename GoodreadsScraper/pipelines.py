# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
from scrapy.exporters import JsonLinesItemExporter
from scrapy import signals


class JsonLineItemSegregator(object):
    @classmethod
    def from_crawler(cls, crawler):
        output_file_suffix = crawler.settings.get("OUTPUT_FILE_SUFFIX", default="")
        return cls(crawler, output_file_suffix)

    def __init__(self, crawler, output_file_suffix):
        self.types = {"book", "author"}
        self.output_file_suffix = output_file_suffix
        self.files = {}
        self.exporters = {}
        self.seen_urls = set() # <--- New: Set to track what we already have

        crawler.signals.connect(self.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(self.spider_closed, signal=signals.spider_closed)

    def spider_opened(self, spider):
        # 1. Open files for appending
        self.files = {name: open(name + "_" + self.output_file_suffix + '.jl', 'a+b') for name in self.types}
        self.exporters = {name: JsonLinesItemExporter(self.files[name]) for name in self.types}

        for e in self.exporters.values():
            e.start_exporting()

        # 2. READ existing files to populate seen_urls (PREVENTS DUPLICATES)
        for name in self.types:
            filename = name + "_" + self.output_file_suffix + '.jl'
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            data = json.loads(line)
                            if 'url' in data:
                                self.seen_urls.add(data['url'])
                        except ValueError:
                            pass # Ignore corrupted lines
            except FileNotFoundError:
                pass # File doesn't exist yet, that's fine

    def spider_closed(self, spider):
        for e in self.exporters.values():
            e.finish_exporting()
        for f in self.files.values():
            f.close()

    def process_item(self, item, spider):
        item_type = type(item).__name__.replace("Item", "").lower()

        # 3. Check if we've seen this URL before writing
        if item_type in self.types:
            if item.get('url') in self.seen_urls:
                return item  # Skip writing, but return item so logs look normal

            # If new, write it and add to memory
            self.exporters[item_type].export_item(item)
            if 'url' in item:
                self.seen_urls.add(item['url'])

        return item
