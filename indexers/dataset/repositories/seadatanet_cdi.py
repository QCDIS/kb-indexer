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
            'contact': raw_doc['Other info']['Quality info'][-1]['Name'],
            'contributor': None,
            'creator': None,
            'description': raw_doc['What?']['Discovery parameter'],
            'discipline': raw_doc['What?']['Discipline'],
            'identifier': None,
            'instrument': raw_doc['How?']['Instrument/gear category'],
            'modification_date': raw_doc['CDI-metadata']['CDI-record last update'],
            'keywords': raw_doc['What?']['Parameter groups'],
            'language': None,
            'publication_year': None,
            'publisher': raw_doc['Other info']['Quality info'][-1]['Name'],
            'related_identifier': None,
            'repo': self.RI,
            'rights': raw_doc['How to get data?']['Access restriction'],
            'size': None,
            'source': [metadata['url']],
            'spatial_coverage': raw_doc['Where?'].get('Sea regions'),
            'temporal_coverage': None,
            'title': raw_doc['What?']['Data set name'],
            'version': None,
            'essential_variables': None,
            'potential_topics': None,
            }

        self.language_extraction(raw_doc, converted_doc)
        self.post_process_doc(converted_doc)
        self.save_index_record(converted_doc, converted_filename)


class SeaDataNetCDIRepository(Repository):
    name = 'SeaDataNet CDI'

    downloader = SeaDataNetCDIDownloader
    converter = SeaDataNetCDIConverter
    indexer = Indexer
