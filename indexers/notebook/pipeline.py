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

    def index(self, index_type):
        print(f'Ingesting notebooks from {self.repo.name}')

        indexes = {
            'metadata': ('notebooks', 'repositories_metadata'),
            'summary': ('notebooks_summary', 'notebooks_summaries'),
            'contents': ('notebooks_contents', 'notebooks_contents'),
            }
        if index_type not in indexes:
            raise ValueError(f'Unknown index type: {index_type}')
        index_name, docs_directory = indexes[index_type]

        data_dir = os.path.join(
            utils.get_data_dir(),
            'notebook',
            self.repo.name,
            docs_directory,
            )
        self.repo.indexer(index_name, data_dir).bulk_ingest()

    def run(self):
        self.search()
        self.index('metadata')
        # self.download()
        # self.preprocess()
        # self.index('summary')
        # self.index('contents')
