import os

import pandas as pd

from .. import utils
from . import repositories


class Pipeline:

    def __init__(self, repo):
        self.repo = repo

    def search(self):
        print(f'Search notebooks from {self.repo.name}')

        df_queries = pd.read_csv(
            os.path.join(
                os.path.dirname(__file__),
                'data_sources/envri_queries.csv'
                )
            )
        queries = df_queries['queries'].values
        self.repo.searcher().bulk_search(queries, page_range=10)

    def download(self):
        print(f'Download notebooks from {self.repo.name}')
        self.repo.downloader().bulk_download()

    def preprocess(self):
        print(f'Preprocess notebooks {self.repo.name}')

        p = self.repo.preprocessor(self.repo.name)
        p.dump_raw_notebooks()
        p.add_new_features()

    def index(self):
        print(f'Ingesting notebooks from {self.repo.name}')

        data_dir = os.path.join(utils.get_data_dir(), 'notebook')
        data_dir = os.path.join(
            data_dir, self.repo.name, 'repositories_metadata')
        self.repo.indexer('notebooks', data_dir).bulk_ingest()

    def run(self):
        self.search()
        # self.download()
        # self.preprocess()
        self.index()
