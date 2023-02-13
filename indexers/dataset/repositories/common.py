from ..download import Downloader
from ..index import Indexer
from ..convert import Converter


class Repository:
    name: str
    downloader: type(Downloader)
    converter: type(Converter)
    indexer: type(Indexer)
