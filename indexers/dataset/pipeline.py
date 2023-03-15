import abc

from .common import Paths


class Pipeline(abc.ABC):

    def __init__(self, repo, reindex=False, keep_files=False):
        self.repo = repo
        self.reindex = reindex
        self.keep_files = keep_files

        self.paths = Paths(self.repo.name)

    def download(self, max_records=None):
        print(f'Download records from {self.repo.name}')
        self.repo.downloader(self.paths).download_all(
            reindex=self.reindex,
            max_records=max_records,
            )

    def convert(self):
        print(f'Converting records from {self.repo.name}')
        self.repo.converter(self.paths).convert_all(keep_files=self.keep_files)

    def index(self):
        print(f'Ingesting records {self.repo.name}')
        self.repo.indexer(self.paths).ingest_all(keep_files=self.keep_files)

    def run(self):
        self.download()
        self.convert()
        self.index()
