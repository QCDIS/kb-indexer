import abc

from tqdm import tqdm

from .common import Paths
from .repositories.icos import ICOSRepository
from .repositories.lifewatch import LifeWatchRepository
from .repositories.seadatanet_cdi import SeaDataNetCDIRepository
from .repositories.seadatanet_edmed import SeaDataNetEDMEDRepository
from .repositories.sios import SIOSRepository


class Pipeline(abc.ABC):

    def __init__(self, repo):
        self.repo = repo
        paths = Paths(self.repo.name)
        self.downloader = self.repo.downloader(paths)
        self.converter = self.repo.converter(paths)
        self.indexer = self.repo.indexer(paths)

    def run(self):
        self.downloader.download_all()
        self.converter.convert_all()
        self.indexer.ingest_all()


def main():
    Pipeline(SeaDataNetCDIRepository).run()
    Pipeline(SeaDataNetEDMEDRepository).run()
    Pipeline(ICOSRepository).run()
    # Pipeline(LifeWatchRepository).run()
    Pipeline(SIOSRepository).run()


if __name__ == '__main__':
    main()
