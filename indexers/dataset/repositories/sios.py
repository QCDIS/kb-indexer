import json

from retrying import retry
from tqdm import tqdm
import urllib.error
import urllib.request

from .common import Repository
from ..download import Downloader
from ..convert import Converter
from ..index import Indexer


class SIOSDownloader(Downloader):
    documents_list_url = 'https://sios.csw.met.no/collections/metadata:main/items'
    document_extension = '.json'

    @retry(retry_on_exception=lambda e: isinstance(e, (urllib.error.HTTPError,
                                                       urllib.error.URLError)),
           wrap_exception=True,
           stop_max_attempt_number=4,
           wait_fixed=5000,
           )
    def _download_page(self, offset, reindex=False):
        url = f'{self.documents_list_url}?f=json&startindex={offset}'
        with urllib.request.urlopen(url) as r:
            response = json.load(r)

        for feature in response['features']:
            url = f"{self.documents_list_url}/{feature['id']}"
            if reindex or not self.indexer.url_is_indexed(url):
                meta = self.gen_metadata(url)
                self.save_metadata(meta)
                with open(meta['filename'], 'w') as f:
                    json.dump(feature, f)

        return response

    def download_all(self, reindex=False):
        response = self._download_page(0, reindex=reindex)
        offset = response['numberReturned']
        with tqdm(desc='downloading records',
                  total=response['numberMatched']) as pbar:
            while response['numberReturned']:
                pbar.update(response['numberReturned'])
                response = self._download_page(offset, reindex=reindex)
                offset += response['numberReturned']


class SIOSConverter(Converter):
    contextual_text_fields = ["title", "keywords", "description"]
    contextual_text_fallback_field = "description"
    RI = 'SIOS'

    def convert_record(self, raw_filename, converted_filename, metadata):
        with open(raw_filename) as f:
            raw_doc = json.load(f)

        if 'properties' not in raw_doc:
            print('no properties', metadata['url'])
            return
        for k in ['extents', 'title', 'recordUpdated', 'keywords', 'description']:
            if k not in raw_doc['properties']:
                print(f'no properties.{k}', metadata['url'])
                return
        if 'spatial' not in raw_doc['properties']['extents']:
            print(f'no properties.extents.spatial', metadata['url'])
            return

        creator = 'Norwegian Meteorological Institute'
        spatial_extent = str(raw_doc['properties']['extents']['spatial'])

        converted_doc = {
            'url': metadata['url'],
            'ResearchInfrastructure': self.RI,
            'name': raw_doc['properties']['title'],
            'copyrightHolder': creator,
            'contributor': creator,
            'creator': creator,
            'publisher': creator,
            'author': creator,
            'producer': creator,
            'provider': creator,
            'contact': 'adc-support@met.no',
            'spatialCoverage': spatial_extent,
            'modificationDate': raw_doc['properties']['recordUpdated'],
            'keywords': raw_doc['properties']['keywords'],
            'description': raw_doc['properties']['description'],
            'abstract': raw_doc['properties']['description'],
            }

        if 'associations' in raw_doc:
            for link in raw_doc['associations']:
                if link['type'] == 'WWW:DOWNLOAD-1.0-http--download':
                    converted_doc['distributionInfo'] = link['href']
                    break
            if ('distributionInfo' not in converted_doc
                    and raw_doc['associations']):
                fallback_link = raw_doc['associations'][0]['href']
                converted_doc['distributionInfo'] = fallback_link

        self.language_extraction(raw_doc, converted_doc)
        self.post_process_doc(converted_doc)
        self.save_index_record(converted_doc, converted_filename)


class SIOSRepository(Repository):
    name = 'SIOS'

    downloader = SIOSDownloader
    converter = SIOSConverter
    indexer = Indexer
