import json

from retrying import retry
from tqdm import tqdm
import urllib.error
import urllib.request

from .common import Repository
from ..download import Downloader
from ..convert import Converter
from ..index import Indexer


class DiSSCoDownloader(Downloader):
    documents_list_url = 'https://sandbox.dissco.tech'
    document_extension = '.json'
    page_size = 100

    @retry(retry_on_exception=lambda e: isinstance(e, (urllib.error.HTTPError,
                                                       urllib.error.URLError)),
           wrap_exception=True,
           stop_max_attempt_number=4,
           wait_fixed=5000,
           )
    def _download_page(self, page_nr, reindex=False):
        url = (f'{self.documents_list_url}/api/v1/specimens'
               f'?pageNumber={page_nr}&pageSize={self.page_size}')
        with urllib.request.urlopen(url) as r:
            response = json.load(r)

        for specimen in response:
            url = f"https://hdl.handle.net/{specimen['id']}"
            if reindex or not self.indexer.url_is_indexed(url):
                meta = self.gen_metadata(url)
                self.save_metadata(meta)
                # Note: also available as jsonld at
                # {self.documents_list_url}/api/v1/specimens/{id}/jsonld
                # BUT this requires to run an extra query for each specimen,
                # making things 50x slower (500 dl/s to 10 dl/s).
                # Given that the json and jsonld contain the same values,
                # it's safe to index the json records. Compare eg:
                # https://sandbox.dissco.tech/api/v1/specimens/20.5000.1025/2F1-DA9-ET4
                # https://sandbox.dissco.tech/api/v1/specimens/20.5000.1025/2F1-DA9-ET4/jsonld
                with open(meta['filename'], 'w') as f:
                    json.dump(specimen, f)
        return response

    def download_all(self, reindex=False):
        response = self._download_page(0, reindex=reindex)
        page = 1
        with tqdm(desc='downloading records') as pbar:
            while len(response):
                pbar.update(len(response))
                response = self._download_page(page, reindex=reindex)
                page += 1


class DiSSCoConverter(Converter):
    contextual_text_fields = ['']
    contextual_text_fallback_field = ''
    RI = 'DiSSCo'

    def convert_record(self, raw_filename, converted_filename, metadata):
        raise NotImplementedError


class DiSSCoRepository(Repository):
    name = 'DiSSCo'

    downloader = DiSSCoDownloader
    converter = DiSSCoConverter
    indexer = Indexer
