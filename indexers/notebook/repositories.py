from typing import Type

from . import searching
from . import downloading
from . import preprocessing
from . import indexing


class Repository:
    name: str
    searcher: Type[searching.NotebookSearcher]
    downloader: Type[downloading.NotebookDownloader]
    preprocessor: Type[preprocessing.RawNotebookPreprocessor]
    indexer: Type[indexing.JsonIndexer]


class KaggleRepository(Repository):
    name = 'Kaggle'
    searcher = searching.KaggleNotebookSearcher
    downloader = downloading.KaggleNotebookDownloader
    preprocessor = preprocessing.RawNotebookPreprocessor
    indexer = indexing.JsonIndexer


class GitHubRepository(Repository):
    name = 'GitHub'
    searcher = searching.GithubNotebookSearcher
    downloader = downloading.GithubNotebookDownloader
    preprocessor = preprocessing.RawNotebookPreprocessor
    indexer = indexing.JsonIndexer
