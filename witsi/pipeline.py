import json
import hashlib
from pathlib import Path
from datetime import datetime

import pandas as pd
from scrapy.exporters import CsvItemExporter


class CsvPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.spider.custom_settings

        spider_name = crawler.spider.name
        proyect = settings.get('BOT_NAME', spider_name)
        output_dir = settings.get('OUTPUT_DIR', './')
        file_path = Path() / output_dir / proyect / f'{spider_name}.csv'

        try:
            header = settings['CSV']['HEADER']
        except KeyError:
            item_class = crawler.spider.item_class
            fields = item_class.__fields__
            fields = [fields[key] for key in fields]
            header = [field.name for field in fields]

        try:
            sort_by = settings['CSV']['SORT_BY']
        except KeyError:
            sort_by = None

        try:
            sort_ascending = settings['CSV']['SORT_ASCENDING']
        except KeyError:
            sort_ascending = True

        return cls(
            file_path=file_path,
            header=header,
            sort_by=sort_by,
            sort_ascending=sort_ascending
        )

    def __init__(self, file_path, header, sort_by, sort_ascending):
        self.sort_by = sort_by
        self.file_path = file_path
        self.sort_ascending = sort_ascending

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
            df = df.sort_values(by=self.sort_by, ascending=self.sort_ascending)

        df.to_csv(self.file_path, index=False)

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


class DataPackagePipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.spider.custom_settings
        item_class = crawler.spider.item_class

        spider_name = crawler.spider.name
        proyect = settings.get('BOT_NAME', spider_name)
        output_dir = settings.get('OUTPUT_DIR', './')
        file_path = Path() / output_dir / proyect / f'{spider_name}.csv'
        datapackage_path = Path() / output_dir / proyect / 'datapackage.json'

        try:
            header = settings['CSV']['HEADER']
        except KeyError:
            fields = item_class.__fields__
            fields = [fields[key] for key in fields]
            header = [field.name for field in fields]

        # Generate the data schema
        keys = item_class.__fields__.keys()
        properties = item_class.schema()['properties']
        fields = [{'name': key, **properties[key]}
                  for key in keys if key in header]

        # Data package base config
        config = settings.get('DATA_PACKAGE', {})

        if 'NAME' not in config:
            config['NAME'] = spider_name

        return cls(
            file_path=file_path,
            datapackage_path=datapackage_path,
            config=config,
            fields=fields,
        )

    def __init__(self, file_path, datapackage_path, config, fields):
        self.file_path = file_path
        self.datapackage_path = datapackage_path
        self.config = config
        self.fields = fields

        if not self.datapackage_path.is_file():
            with open(self.datapackage_path, 'w', encoding='utf-8') as file:
                base_package = {
                    'name': self.config['NAME'],
                    'title': self.config.get('TITLE', ''),
                    'description': self.config.get('DESCRIPTION', ''),
                    'resources': []
                }
                json.dump(base_package, file, ensure_ascii=False, indent=4)

    def close_spider(self, spider):
        with open(self.datapackage_path) as file:
            datapackage = json.load(file)

        index = next((index for (index, resource)
                     in enumerate(datapackage['resources'])
                     if resource["name"] == spider.name), -1)

        if index < 0:
            index = len(datapackage['resources'])
            datapackage['resources'].append({})

        md5sum = hashlib.md5(open(self.file_path, 'r').read()
                             .encode()).hexdigest()

        resource = datapackage['resources'][index]
        resource['name'] = spider.name
        resource['hash'] = f'md5-{md5sum}'
        resource['bytes'] = self.file_path.stat().st_size
        resource['last_updated'] = datetime.now().isoformat()
        resource['fields'] = self.fields

        datapackage['resources'][index] = resource

        with open(self.datapackage_path, 'w') as file:
            json.dump(datapackage, file, indent=2, ensure_ascii=False)

    def process_item(self, item, spider):
        return item
