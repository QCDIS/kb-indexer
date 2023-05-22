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

        for specimen in response['data']:
            url = specimen['id']
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

    def download_all(self, reindex=False, max_records=None, offset=0):
        page = offset // self.page_size
        response = self._download_page(page, reindex=reindex)
        with tqdm(desc='downloading records') as pbar:
            while len(response):
                pbar.update(len(response))
                if (max_records is not None) and (pbar.n >= max_records):
                    return
                page += 1
                response = self._download_page(page, reindex=reindex)


class DiSSCoConverter(Converter):
    contextual_text_fields = ['specimenName', 'type', 'dwc:basisOfRecord'
                              'dwc:occurrenceStatus', 'dwc:occurrenceRemarks',
                              'dwc:class', 'dwc:order', 'dwc:genus',
                              'dwc:family', 'dwc:specificEpiteth',
                              'dwc:preparations',
                              ]
    contextual_text_fallback_field = 'specimenName'
    RI = 'DiSSCo'

    def convert_record(self, raw_filename, converted_filename, metadata):
        with open(raw_filename) as f:
            raw_doc = json.load(f)

        if 'specimenName' not in raw_doc:
            print(f"skipping {metadata['url']}, no specimenName")
            return

        converted_doc = {
            'contact': None,
            'contributor': raw_doc['data'].get('dwc:identifiedBy'),
            'creator': raw_doc['data'].get('dwc:recordedBy'),
            'description': raw_doc['type'],
            'discipline': None,
            'identifier': f"https://hdl.handle.net/{raw_doc['id']}",
            'instrument': raw_doc['data'].get('dwc:basisOfRecord', 'Specimen'),
            'modification_date': raw_doc['created'],
            'keywords': None,
            'language': None,
            'publication_year': None,
            'publisher': self._resolve_organization(raw_doc['organizationId']),
            'related_identifier': raw_doc['data'].get('dwc:catalogNumber'),
            'repo': self.RI,
            'rights': raw_doc['data'].get('dcterms:bibliographicCitation'),
            'size': None,
            'source': self._landing_page(raw_doc['id']),
            'spatial_coverage': self._extract_location(raw_doc),
            'temporal_coverage': self._extract_temporal_coverage(raw_doc),
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

    @staticmethod
    def _extract_location(doc):
        keywords = [
            'dwc:locality',
            'dwc:municipality',
            'dwc:county',
            'dwc:stateProvince',
            'dwc:country',
            'dwc:higherGeography',
            'dwc:continent',
            ]
        values = [doc['data'].get(k) for k in keywords]
        values = [v for v in values if v is not None]
        if not values:
            return None
        return values

    @staticmethod
    def _extract_temporal_coverage(doc):
        keywords = [
            'dwc:earliestEonOrLowestEonothem',
            'dwc:earliestEraOrLowestErathem',
            'dwc:earliestPeriodOrLowestSystem',
            'dwc:earliestEpochOrLowestSeries',
            'dwc:earliestAgeOrLowestStage',
            ]
        values = [doc['data'].get(k) for k in keywords]
        values = [v for v in values if v is not None]
        if not values:
            return None
        return values


class DiSSCoRepository(Repository):
    name = 'DiSSCo'

    downloader = DiSSCoDownloader
    converter = DiSSCoConverter
    indexer = Indexer
