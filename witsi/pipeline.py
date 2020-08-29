import json
import hashlib
from pathlib import Path
from shutil import copyfile
from datetime import datetime

import pandas as pd
from scrapy.exporters import CsvItemExporter


class ToCsvPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.spider.custom_settings

        spider_name = crawler.spider.name
        proyect = settings.get('BOT_NAME', spider_name)
        output_dir = settings.get('OUTPUT_DIR', './')
        file_path = Path() / output_dir / proyect / f'{spider_name}.csv'

        return cls(
            file_path=file_path,
            header=settings['CSV']['HEADER'],
            sort_by=settings['CSV']['SORT_BY'],
        )

    def __init__(self, file_path, header, sort_by):
        self.sort_by = sort_by
        self.file_path = file_path

        # Create the output file
        file_path.parent.mkdir(parents=True, exist_ok=True)
        self.file = open(file_path, 'ab')

        if (file_path.stat().st_size == 0):
            self.file.write(str.encode(','.join(header) + '\n'))

        self.exporter = CsvItemExporter(self.file, include_headers_line=False)
        self.exporter.fields_to_export = header
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

        df = pd.read_csv(self.file_path)
        df = df.drop_duplicates()

        if self.sort_by:
            df = df.sort_values(by=self.sort_by, ascending=True)

        df.to_csv(self.file_path, index=False)

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


class DataPackagedPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.spider.custom_settings

        spider_name = crawler.spider.name
        proyect = settings.get('BOT_NAME', spider_name)
        output_dir = settings.get('OUTPUT_DIR', './')
        file_path = Path() / output_dir / proyect / f'{spider_name}.csv'
        datapackage_path = Path() / output_dir / proyect / 'datapackage.json'

        return cls(
            file_path=file_path,
            datapackage_path=datapackage_path
        )

    def __init__(self, file_path, datapackage_path):
        self.file_path = file_path
        self.datapackage_path = datapackage_path

        if not datapackage_path.is_file():
            copyfile('./datapackage.json', self.datapackage_path)

    def close_spider(self, spider):
        with open(self.datapackage_path) as file:
            datapackage = json.load(file)

        for i, resource in enumerate(datapackage['resources']):
            if resource['name'] == spider.name:
                break

        md5sum = hashlib.md5(open(self.file_path, 'r').read().encode()).hexdigest()

        datapackage['resources'][i]['hash'] = md5sum
        datapackage['resources'][i]['bytes'] = self.file_path.stat().st_size
        datapackage['resources'][i]['last_updated'] = datetime.now().isoformat()

        with open(self.datapackage_path, 'w') as file:
            json.dump(datapackage, file, indent=2, ensure_ascii=False)

    def process_item(self, item, spider):
        return item
