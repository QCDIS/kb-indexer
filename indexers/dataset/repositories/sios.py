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
            break
        print('done')


class SIOSConverter(Converter):
    contextual_text_fields = ["title", "keywords", "description"]
    contextual_text_fallback_field = "description"
    RI = 'SIOS'

    def convert_record(self, raw_filename, converted_filename, metadata):

        with open(raw_filename) as f:
            dataset_metadata = json.load(f)

        if 'properties' not in dataset_metadata:
            print('no properties', metadata['url'])
            return
        for k in ['extents', 'title', 'recordUpdated', 'keywords', 'description']:
            if k not in dataset_metadata['properties']:
                print(f'no properties.{k}', metadata['url'])
                return
        if 'spatial' not in dataset_metadata['properties']['extents']:
            print(f'no properties.extents.spatial', metadata['url'])
            return

        creator = 'Norwegian Meteorological Institute'
        spatial_extent = str(dataset_metadata['properties']['extents']['spatial'])

        index_record = {
            'url': metadata['url'],
            'ResearchInfrastructure': self.RI,
            'name': dataset_metadata['properties']['title'],
            'copyrightHolder': creator,
            'contributor': creator,
            'creator': creator,
            'publisher': creator,
            'author': creator,
            'producer': creator,
            'provider': creator,
            'contact': 'adc-support@met.no',
            'spatialCoverage': spatial_extent,
            'modificationDate': dataset_metadata['properties']['recordUpdated'],
            'keywords': dataset_metadata['properties']['keywords'],
            'description': dataset_metadata['properties']['description'],
            'abstract': dataset_metadata['properties']['description'],
            }

        if 'associations' in dataset_metadata:
            for link in dataset_metadata['associations']:
                if link['type'] == 'WWW:DOWNLOAD-1.0-http--download':
                    index_record['distributionInfo'] = link['href']
                    break
            if ('distributionInfo' not in index_record
                    and dataset_metadata['associations']):
                fallback_link = dataset_metadata['associations'][0]['href']
                index_record['distributionInfo'] = fallback_link

        index_record["potentialTopics"] = self.topicMining(dataset_metadata)

        index_record["EssentialVariables"] = self.getDomainEssentialVariables(
            self.getDomain(self.RI)[0])
        index_record["EssentialVariables"] = self.getSimilarEssentialVariables(
            index_record["EssentialVariables"],
            index_record["potentialTopics"],
            )

        index_record = self.post_process_index_record(index_record)
        self.save_index_record(index_record, converted_filename)


class SIOSRepository(Repository):
    name = 'SIOS'
    research_infrastructure = 'SIOS'

    downloader = SIOSDownloader
    converter = SIOSConverter
    indexer = Indexer
