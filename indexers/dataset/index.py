from glob import glob
import json
import os

from tqdm import tqdm

from .. import utils
from .common import Paths


class Indexer:
    def __init__(self, paths: Paths):
        self.paths = paths
        self.indexer = utils.ElasticsearchIndexer('envri')

    def list_index_record_files(self):
        pattern = os.path.join(self.paths.index_records_dir, '*.json')
        return sorted(glob(pattern))

    def clear_index_record_files(self):
        for filename in self.list_index_record_files():
            os.remove(filename)

    def ingest_record_files(self):
        for record_file in tqdm(
                self.list_index_record_files(),
                desc='ingesting indexes'
                ):
            try:
                with open(record_file, 'r') as f:
                    record = json.load(f)
            except json.decoder.JSONDecodeError:
                print('skipping', record_file)
                continue

            id_ = utils.gen_id_from_url(record['url'])
            self.indexer.ingest_record(id_, record)

    def url_is_indexed(self, url):
        return self.indexer.is_in_index('url', url)
