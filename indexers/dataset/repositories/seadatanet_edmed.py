import json
import re
import string

from bs4 import BeautifulSoup
import urllib.request

from .common import Repository
from ..download import TwoStepDownloader
from ..convert import Converter
from ..index import Indexer


class SeaDataNetEDMEDDownloader(TwoStepDownloader):
    documents_list_url = 'https://edmed.seadatanet.org/sparql/sparql?query=select+%3FEDMEDRecord+%3FTitle+where+%7B%3FEDMEDRecord+a+%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fdcat%23Dataset%3E+%3B+%3Chttp%3A%2F%2Fpurl.org%2Fdc%2Fterms%2Ftitle%3E+%3FTitle+.%7D+&output=json&stylesheet='
    document_extension = '.html'

    def get_documents_urls(self):
        with urllib.request.urlopen(self.documents_list_url) as r:
            data = json.load(r)
        urls = [record["EDMEDRecord"]["value"]
                for record in data["results"]["bindings"]]
        return urls


class SeaDataNetEDMEDConverter(Converter):
    contextual_text_fields = ["name", "keywords", "measurementTechnique"]
    contextual_text_fallback_field = "abstract"
    RI = "SeaDataNet"

    def __init__(self, paths):
        super().__init__(paths)

    @staticmethod
    def _cleanhtml(raw_html):
        CLEANR = re.compile('<.*?>')
        cleantext = re.sub(CLEANR, '', raw_html)
        res = ''.join(x for x in cleantext if x in string.printable)
        return res.replace("'", "").replace("\"", "").strip()

    def convert_record(self, raw_filename, converted_filename, metadata):
        with open(raw_filename, 'rb') as f:
            soup = BeautifulSoup(f, 'lxml')

        raw_doc = dict()
        for tr in soup.find_all('tr'):
            th = tr.findChild('th')
            if th:
                raw_doc[th.text] = tr.findChild('td').text

        converted_doc = {
            'url': metadata['url'],
            'name': raw_doc['Data set name'],
            'ResearchInfrastructure': 'SeaDataNet',
            'copyrightHolder': raw_doc['Data holding centre'],
            'contributor': raw_doc['Data holding centre'],
            'locationCreated': raw_doc['Country'],
            'contentLocation': raw_doc['Country'],
            "contentReferenceTime": raw_doc["Time period"],
            "datePublished": raw_doc["Time period"],
            "dateCreated": raw_doc["Time period"],
            "spatialCoverage": raw_doc["Geographical area"],
            "keywords": raw_doc["Parameters"],
            "measurementTechnique": raw_doc["Instruments"],
            "description": raw_doc["Summary"],
            "abstract": raw_doc["Summary"],
            "creator": raw_doc["Originators"],
            "distributionInfo": raw_doc["Data holding centre"],
            "publisher": raw_doc["Organisation"],
            "author": raw_doc["Contact"],
            "contact": raw_doc["Address"],
            "producer": raw_doc["Collating centre"],
            "provider": raw_doc["Collating centre"],
            "identifier": raw_doc["Local identifier"],
            "modificationDate": raw_doc["Last revised"],
            }

        self.language_extraction(raw_doc, converted_doc)
        self.post_process_doc(converted_doc)
        self.save_index_record(converted_doc, converted_filename)


class SeaDataNetEDMEDRepository(Repository):
    name = 'SeaDataNet EDMED'

    downloader = SeaDataNetEDMEDDownloader
    converter = SeaDataNetEDMEDConverter
    indexer = Indexer
