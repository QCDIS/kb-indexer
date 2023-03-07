import json
import os

from tqdm import tqdm

from .. import utils


class JsonIndexer(utils.ElasticsearchIndexer):

    def __init__(self, index_name, json_dir):
        super().__init__(index_name)
        self.json_dir = json_dir

    def bulk_ingest(self):
        for record_file in tqdm(os.listdir(self.json_dir),
                                desc='ingesting records'):
            record_file = os.path.join(self.json_dir, record_file)
            if not os.path.splitext(record_file)[1] == '.json':
                print('skipping', record_file)
                continue
            with open(record_file, 'r') as f:
                record = json.load(f)
            self.ingest_record(record['id'], record)
