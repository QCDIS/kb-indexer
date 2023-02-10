import abc

from tqdm import tqdm

from .common import Paths


class Downloader(abc.ABC):
    dataset_list_url: str
    metadata_record_ext: str

    @abc.abstractmethod
    def extract_records(self):
        pass


class DirectDownloader(Downloader, abc.ABC):
    pass


class TwoStepDownloader(Downloader, abc.ABC):
    """ 2-step download: 1) list of record URIs 2) download metadata
    """

    def __init__(self, paths: Paths):
        self.paths = paths

    @abc.abstractmethod
    def get_record_urls(self):
        pass

    @abc.abstractmethod
    def download_record(self, url, filename):
        pass

    def extract_records(self):
        for url in tqdm(self.get_record_urls(), desc='extracting records'):
            filename = self.paths.metadata_record_filename(
                self.paths.url_to_id(url), self.metadata_record_ext)
            self.download_record(url, filename)
