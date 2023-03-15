from datetime import datetime
import abc
import json
import textwrap

from tqdm import tqdm
import urllib.request
import urllib.error
import requests

from .common import Paths
from .index import Indexer


class Downloader(abc.ABC):
    documents_list_url: str
    document_extension: str

    def __init__(self, paths: Paths):
        self.paths = paths
        self.indexer = Indexer(paths)

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

    @staticmethod
    def download_url(url, filename):
        try:
            urllib.request.urlretrieve(url, filename)
        except (urllib.error.HTTPError, urllib.error.URLError):
            print(f'Could not open {url}, skipping')

    @abc.abstractmethod
    def download_all(self, reindex=False, max_records=None):
        pass


class SPARQLDownloader(Downloader, abc.ABC):

    @staticmethod
    def sparql_query(endpoint, query, max_records=None):
        if max_records is not None:
            query += f'limit {max_records}\n'
        query = textwrap.dedent(query).strip()

        r = requests.post(
            endpoint,
            headers={
                'Cache-Control': 'no-cache',
                'accept': 'text/csv'},
            data={'query': query},
            )
        return r.text.splitlines()[1:]


class TwoStepDownloader(Downloader, abc.ABC):
    """ 2-step download: 1) list of record URIs 2) download metadata
    """

    @abc.abstractmethod
    def get_documents_urls(self, max_records=None):
        pass

    def download_all(self, reindex=False, max_records=None):
        for url in tqdm(self.get_documents_urls(max_records=max_records),
                        desc='downloading records'):
            if reindex or not self.indexer.url_is_indexed(url):
                meta = self.gen_metadata(url)
                self.save_metadata(meta)
                self.download_url(meta['url'], meta['filename'])
