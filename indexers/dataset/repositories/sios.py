import json
import urllib.error
import urllib.request

from .common import Repository
from ..download import TwoStepDownloader, DirectDownloader
from ..map import Mapper
from ..index import Indexer


class SIOSDownloader(DirectDownloader):
    dataset_list_url = 'https://sios.csw.met.no/collections/metadata:main/items'

    def _get_dataset_list_page(self, i):
        results_per_page = 10  # from API doc
        start_index = i * results_per_page
        url = f'{self.dataset_list_url}?f=json&startindex={start_index}'
        try:
            with urllib.request.urlopen(url) as r:
                response = json.load(r)
            return response
        except (urllib.error.HTTPError, urllib.error.URLError):
            print(f'Could not open {url}, skipping')
            empty_response = {
                'numberReturned': 0,
                'features': [],
                }
            return empty_response

    def list_records(self):
        response = self._get_dataset_list_page(0)
        datasets = response['features']
        page = 1
        print('Listing datasets (this might take a while)')
        while response['numberReturned']:
            response = self._get_dataset_list_page(page)
            datasets += response['features']
            page += 1
        print('done')

        with open(self.paths.dataset_list_filename, 'w') as f:
            json.dump(datasets, f)

    def convert_dataset_list_to_dataset_urls(self):
        with open(self.paths.dataset_list_filename, "r") as f:
            datasets = json.load(f)

        urls = [f"{self.dataset_list_url}/{feature['id']}?f=json"
                for feature in datasets]

        with open(self.paths.dataset_urls_filename, 'w') as f:
            f.write('\n'.join(urls))


class SIOSMapper(Mapper):
    contextual_text_fields = ["title", "keywords", "description"]
    contextual_text_fallback_field = "description"
    RI = 'SIOS'

    def gen_record_from_url(self, url):
        try:
            with urllib.request.urlopen(url) as r:
                dataset_metadata = json.load(r)
        except (urllib.error.HTTPError, urllib.error.URLError):
            print(f'Could not open {url}, skipping')
            return

        if 'properties' not in dataset_metadata:
            print('no properties', url)
            return
        for k in ['extents', 'title', 'recordUpdated', 'keywords', 'description']:
            if k not in dataset_metadata['properties']:
                print(f'no properties.{k}', url)
                return
        if 'spatial' not in dataset_metadata['properties']['extents']:
            print(f'no properties.extents.spatial', url)
            return

        creator = 'Norwegian Meteorological Institute'
        spatial_extent = str(dataset_metadata['properties']['extents']['spatial'])

        index_record = {
            'url': url,
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
        self.save_index_record(index_record)


class SIOSRepository(Repository):
    name = 'SIOS'
    research_infrastructure = 'SIOS'

    downloader = SIOSDownloader
    mapper = SIOSMapper
    indexer = Indexer
