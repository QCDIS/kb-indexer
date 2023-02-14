import json

from xml.etree import ElementTree
import urllib.request

from .common import Repository
from ..download import TwoStepDownloader
from ..convert import Converter
from ..index import Indexer


class SeaDataNetCDIDownloader(TwoStepDownloader):
    documents_list_url = 'https://cdi.seadatanet.org/report/aggregation'
    document_extension = '.json'

    def get_documents_urls(self):
        with urllib.request.urlopen(self.documents_list_url) as r:
            tree = ElementTree.parse(r)
        records_root = tree.getroot()
        urls = []
        for record in records_root:
            url = record.text
            pos = url.rfind("/xml")
            if pos and pos + 4 == len(url):
                url = url.replace("/xml", "/json")
            urls.append(url)
        return urls


class SeaDataNetCDIConverter(Converter):
    contextual_text_fields = [
        "Data set name", "Discipline", "Parameter groups",
        "Discovery parameter", "GEMET-INSPIRE themes"]
    contextual_text_fallback_field = "Abstract"
    RI = 'SeaDataNet'

    def convert_record(self, raw_filename, converted_filename, metadata):
        with open(raw_filename) as f:
            raw_doc = json.load(f)

        converted_doc = {
            'description': raw_doc['What?']['Discovery parameter'],
            'keywords': raw_doc['What?']['Parameter groups'],
            'language': ['English'],
            'accessibilitySummary': raw_doc['How to get data?']['Access/ordering of data'],
            'contact': raw_doc['Other info']['Quality info'][-1]['Name'],
            'publisher': raw_doc['Other info']['Quality info'][-1]['Name'],
            'genre': raw_doc['What?']['Discipline'],
            'modificationDate': raw_doc['CDI-metadata']['CDI-record last update'],
            'abstract': [raw_doc['What?']['Abstract']],
            'spatialCoverage': raw_doc['Where?']['Sea regions'],
            'url': [metadata['url']],
            'name': raw_doc['What?']['Data set name'],
            'measurementTechnique': raw_doc['How?']['Instrument/gear category'],
            'useConstraints': raw_doc['How to get data?']['Access restriction'],
            'scope': raw_doc['Other info']['Lineage'],
            'dataQualityInfo': self._extract_dataQualityInfo(raw_doc),
            'ResearchInfrastructure': self.RI,
            }

        self.language_extraction(raw_doc, converted_doc)
        self.post_process_doc(converted_doc)
        self.save_index_record(converted_doc, converted_filename)

    @staticmethod
    def _extract_dataQualityInfo(doc):
        return [q['Name'] for q in doc['Other info']['Quality info']]


class SeaDataNetCDIRepository(Repository):
    name = 'SeaDataNet CDI'

    downloader = SeaDataNetCDIDownloader
    converter = SeaDataNetCDIConverter
    indexer = Indexer
