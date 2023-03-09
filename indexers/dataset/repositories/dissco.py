import json
import re

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
    contextual_text_fields = ['']  # FIXME
    contextual_text_fallback_field = ''  # FIXME
    RI = 'DiSSCo'

    def convert_record(self, raw_filename, converted_filename, metadata):
        with open(raw_filename) as f:
            raw_doc = json.load(f)

        if 'specimenName' not in raw_doc:
            print(f"skipping {metadata['url']}, no specimenName")
            return

        converted_doc = {
            'contact': None,
            'contributor': None,
            'creator': None,
            'description': raw_doc['type'],
            'discipline': None,
            'identifier': f"https://hdl.handle.net/{raw_doc['id']}",
            'instrument': None,  # FIXME
            'modification_date': raw_doc['created'],
            'keywords': None,
            'language': None,
            'publication_year': None,
            'publisher': self._resolve_organization(raw_doc['organizationId']),
            'related_identifier': None,
            'repo': self.RI,
            'rights': None,
            'size': None,
            'source': self._landing_page(raw_doc['id']),
            'spatial_coverage': None,  # FIXME
            'temporal_coverage': None,
            'title': raw_doc['specimenName'],
            'version': None,
            'essential_variables': None,
            'potential_topics': None,
            }

        self.language_extraction(raw_doc, converted_doc)
        self.post_process_doc(converted_doc)
        self.save_index_record(converted_doc, converted_filename)

    @staticmethod
    def _resolve_organization(org_id):
        org_id = re.match(r'https?://ror\.org/(\w+)', org_id).group(1)
        url = f'https://api.ror.org/organizations/{org_id}'
        with urllib.request.urlopen(url) as r:
            org_data = json.load(r)
        return org_data['name']

    @staticmethod
    def _landing_page(id_):
        return f"https://sandbox.dissco.tech/ds/{id_}"


class DiSSCoRepository(Repository):
    name = 'DiSSCo'

    downloader = DiSSCoDownloader
    converter = DiSSCoConverter
    indexer = Indexer
