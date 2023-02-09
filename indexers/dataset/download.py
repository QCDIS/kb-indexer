import abc
import urllib.request

from .common import Paths


class Downloader(abc.ABC):
    dataset_list_url: str
    dataset_list_ext: str

    def __init__(self, paths: Paths):
        self.paths = paths

    def get_dataset_list(self):
        urllib.request.urlretrieve(
            self.dataset_list_url, self.paths.dataset_list_filename)

    @abc.abstractmethod
    def convert_dataset_list_to_dataset_urls(self):
        pass

    def get_dataset_urls(self):
        with open(self.paths.dataset_urls_filename, 'r') as f:
            urls = [line.strip() for line in f.readlines()]
        return urls
