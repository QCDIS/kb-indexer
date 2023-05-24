import json

from xml.etree import ElementTree
import urllib.request

from .common import Repository
from ..download import TwoStepDownloader, SPARQLDownloader
from ..convert import Converter
from ..index import Indexer


class SeaDataNetCDIDownloader(TwoStepDownloader, SPARQLDownloader):
    documents_list_url = 'https://cdi.seadatanet.org/sparql/sparql'
    document_extension = '.json'

    def get_documents_urls(self, max_records=None, offset=0):
        query = r"""
        SELECT ?o WHERE {
            ?s <http://www.w3.org/ns/dcat#dataset> ?o
        }
        """
        urls = self.sparql_query(
            self.documents_list_url,
            query,
            max_records=max_records,
            offset=offset,
            )[:-1]
        return [f'{url}/json' for url in urls]


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
