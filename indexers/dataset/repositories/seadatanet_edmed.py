import re
import string

from bs4 import BeautifulSoup

from .common import Repository
from ..download import TwoStepDownloader, SPARQLDownloader
from ..convert import Converter
from ..index import Indexer


class SeaDataNetEDMEDDownloader(TwoStepDownloader, SPARQLDownloader):
    documents_list_url = 'https://edmed.seadatanet.org/sparql/sparql'
    document_extension = '.html'

    def get_documents_urls(self, max_records=None, offset=0):
        query = r"""
        select ?EDMEDRecord
        where {
            ?EDMEDRecord a <http://www.w3.org/ns/dcat#Dataset>
        }
        """
        return self.sparql_query(
            self.documents_list_url,
            query,
            max_records=max_records,
            offset=offset,
            )


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
            d = f.read().decode('latin1')
            soup = BeautifulSoup(d, 'lxml')

        raw_doc = dict()
        for tr in soup.find_all('tr'):
            th = tr.findChild('th')
            if th:
                raw_doc[th.text] = tr.findChild('td').text

        converted_doc = {
            'contact': raw_doc.get('Contact'),
            'contributor': raw_doc.get('Data holding centre'),
            'creator': raw_doc.get('Organisation'),
            'description': raw_doc.get('Summary'),
            'discipline': None,
            'identifier': raw_doc.get('Local identifier'),
            'instrument': raw_doc.get('Instruments'),
            'modification_date': raw_doc.get('Last revised'),
            'keywords': raw_doc.get('Parameters'),
            'language': None,
            'publication_year': raw_doc.get('Time period'),
            'publisher': raw_doc.get('Data holding centre'),
            'related_identifier': None,
            'repo': self.RI,
            'rights': None,
            'size': None,
            'source': metadata['url'],
            'spatial_coverage': raw_doc.get('Geographical area'),
            'temporal_coverage': raw_doc.get('Time period'),
            'title': raw_doc['Data set name'],
            'version': None,
            'essential_variables': None,
            'potential_topics': None,
            }

        self.language_extraction(raw_doc, converted_doc)
        self.post_process_doc(converted_doc)
        self.save_index_record(converted_doc, converted_filename)


class SeaDataNetEDMEDRepository(Repository):
    name = 'SeaDataNet EDMED'

    downloader = SeaDataNetEDMEDDownloader
    converter = SeaDataNetEDMEDConverter
    indexer = Indexer
