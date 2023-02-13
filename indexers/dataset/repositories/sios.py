import json

import urllib.error
import urllib.request

from .common import Repository
from ..download import Downloader
from ..convert import Converter
from ..index import Indexer


class SIOSDownloader(Downloader):
    documents_list_url = 'https://sios.csw.met.no/collections/metadata:main/items'
    document_extension = '.json'

    def _download_page(self, i):
        results_per_page = 10  # from API doc
        start_index = i * results_per_page
        url = f'{self.documents_list_url}?f=json&startindex={start_index}'
        try:
            with urllib.request.urlopen(url) as r:
                response = json.load(r)
        except (urllib.error.HTTPError, urllib.error.URLError):
            print(f'Could not open {url}, skipping')
            response = {
                'numberReturned': 0,
                'features': [],
                }
        for feature in response['features']:
            url = f"{self.documents_list_url}/{feature['id']}?f=json"
            meta = self.gen_metadata(url)
            self.save_metadata(meta)
            self.download_url(meta['url'], meta['filename'])

        return response

    def download_all(self):
        response = self._download_page(0)
        page = 1
        print('Downloading datasets (this might take a while)')
        while response['numberReturned']:
            response = self._download_page(page)
            page += 1
        print('done')


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
