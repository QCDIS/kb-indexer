from ..download import Downloader
from ..index import Indexer
from ..map import Mapper


class Repository:
    name: str
    research_infrastructure: str
    downloader: type(Downloader)
    mapper: type(Mapper)
    indexer: type(Indexer)
