from datetime import datetime
import abc
import json

from tqdm import tqdm
import urllib.request
import urllib.error

from .common import Paths


class Downloader(abc.ABC):
    documents_list_url: str
    document_extension: str

    def __init__(self, paths: Paths):
        self.paths = paths

    def gen_metadata(self, url):
        id_ = self.paths.url_to_id(url)
        meta = {
            'id': id_,
            'url': url,
            'filename': self.paths.raw_file(id_, self.document_extension),
            'retrieval_time': datetime.now().isoformat(),
            }
        return meta

    def save_metadata(self, meta):
        with open(self.paths.meta_file(meta['id']), 'w') as f:
            json.dump(meta, f)

    @abc.abstractmethod
    def download_all(self):
        pass


class DirectDownloader(Downloader, abc.ABC):
    pass


class TwoStepDownloader(Downloader, abc.ABC):
    """ 2-step download: 1) list of record URIs 2) download metadata
    """

    @abc.abstractmethod
    def get_documents_urls(self):
        pass

    @staticmethod
    def download_url(url, filename):
        try:
            urllib.request.urlretrieve(url, filename)
        except urllib.error.HTTPError:
            print(f'Could not open {url}, skipping')

    def download_all(self):
        for url in tqdm(self.get_documents_urls(), desc='downloading records'):
            meta = self.gen_metadata(url)
            self.save_metadata(meta)
            self.download_url(meta['url'], meta['filename'])
