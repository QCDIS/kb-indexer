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
        self.mapper = self.repo.mapper(paths)
        self.indexer = self.repo.indexer(paths)

    def run(self):
        self.indexer.clear_index_record_files()

        print(f'indexing the {self.repo.name} dataset repository')
        self.downloader.get_dataset_list()
        self.downloader.convert_dataset_list_to_dataset_urls()

        urls = self.downloader.get_dataset_urls()
        for url in tqdm(urls, desc='generating dataset records'):
            if not self.indexer.url_is_indexed(url):
                self.mapper.gen_record_from_url(url)
                self.indexer.ingest_record_files()
                self.indexer.clear_index_record_files()


def main():
    Pipeline(SeaDataNetCDIRepository).run()
    Pipeline(SeaDataNetEDMEDRepository).run()
    Pipeline(ICOSRepository).run()
    # Pipeline(LifeWatchRepository).run()
    Pipeline(SIOSRepository).run()


if __name__ == '__main__':
    main()
