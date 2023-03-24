from glob import glob
import json
import os

from tqdm import tqdm

from .. import utils
from .common import Paths


class Indexer:
    def __init__(self, paths: Paths):
        self.paths = paths
        self.indexer = utils.ElasticsearchIndexer('dataset')

    def list_files(self):
        pattern = self.paths.converted_file('*')
        return sorted(glob(pattern))

    def clear_files(self):
        for filename in self.list_files():
            os.remove(filename)

    def ingest_all(self, keep_files=False):
        for doc_filename in tqdm(self.list_files(), desc='ingesting records'):
            try:
                with open(doc_filename, 'r') as f:
                    doc = json.load(f)
            except json.decoder.JSONDecodeError:
                print('skipping', doc_filename)
                continue
            id_ = utils.gen_id_from_url(doc['source'])
            self.indexer.ingest_record(id_, doc)
            if not keep_files:
                os.remove(doc_filename)
        self.indexer.refresh_index()

    def url_is_indexed(self, url):
        return self.indexer.is_in_index('source', url)
