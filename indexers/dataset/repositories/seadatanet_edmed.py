import json
import os
import re
import string

import requests.exceptions

from ... import utils
from .common import Repository
from ..download import Downloader
from ..map import Mapper, get_html_tags
from ..index import Indexer


class SeaDataNetEDMEDDownloader(Downloader):
    dataset_list_url = 'https://edmed.seadatanet.org/sparql/sparql?query=select+%3FEDMEDRecord+%3FTitle+where+%7B%3FEDMEDRecord+a+%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fdcat%23Dataset%3E+%3B+%3Chttp%3A%2F%2Fpurl.org%2Fdc%2Fterms%2Ftitle%3E+%3FTitle+.%7D+&output=json&stylesheet='
    dataset_list_ext = ".json"

    def convert_dataset_list_to_dataset_urls(self):
        with open(self.paths.dataset_list_filename, "r") as f:
            data = json.load(f)

        urls = [record["EDMEDRecord"]["value"]
                for record in data["results"]["bindings"]]

        with open(self.paths.dataset_urls_filename, 'w') as f:
            f.write('\n'.join(urls))

    def extract_datasets(self):
        raise NotImplemented  # TODO


class SeaDataNetEDMEDMapper(Mapper):
    contextual_text_fields = ["name", "keywords", "measurementTechnique"]
    contextual_text_fallback_field = "abstract"

    def __init__(self, paths):
        super().__init__(paths)
        self.lstCoveredFeaturesSeaDataNet = []

    @staticmethod
    def _cleanhtml(raw_html):
        CLEANR = re.compile('<.*?>')
        cleantext = re.sub(CLEANR, '', raw_html)
        res = ''.join(x for x in cleantext if x in string.printable)
        return res.replace("'", "").replace("\"", "").strip()

    def getValueHTML(self, searchTerm, datasetContents):
        for datasetContent in datasetContents:
            datasetContent = str(datasetContent)
            if (searchTerm in datasetContent
                    and searchTerm not in self.lstCoveredFeaturesSeaDataNet):
                self.lstCoveredFeaturesSeaDataNet.append(searchTerm)
                return self._cleanhtml(datasetContent)[len(searchTerm):]

    def gen_record_from_url(self, datasetURL):
        try:
            datasetContents = get_html_tags(datasetURL, "tr")
        except requests.exceptions.ConnectionError:
            print(f'Could not open {datasetURL}, skipping')
            return

        with open(self.paths.metadataStar_filename, "r") as f:
            metadataStar_object = json.loads(f.read())

        indexfname = os.path.join(
            self.paths.index_records_dir,
            utils.gen_id_from_url(datasetURL) + '.json',
            )
        indexFile = open(indexfname, "w")
        indexFile.write("{\n")

        originalValues = []

        EDMED_JSON = {}

        self.lstCoveredFeaturesSeaDataNet.clear()
        mapping = {}
        cnt = 0
        mapping["url"] = str(datasetURL)
        mapping["ResearchInfrastructure"] = "SeaDataNet"
        RI = "SeaDataNet"

        value = (self.getValueHTML("Data set name", datasetContents))
        mapping["name"] = str(value)
        EDMED_JSON["name"] = value

        value = (self.getValueHTML("Data holding centre", datasetContents))
        mapping["copyrightHolder"] = str(value)
        mapping["contributor"] = str(value)
        EDMED_JSON["contributor"] = value

        value = (self.getValueHTML("Country", datasetContents))
        mapping["locationCreated"] = str(value)
        mapping["contentLocation"] = str(value)
        EDMED_JSON["contentLocation"] = value

        value = (self.getValueHTML("Time period", datasetContents))
        mapping["contentReferenceTime"] = str(value)
        mapping["datePublished"] = str(value)
        mapping["dateCreated"] = str(value)
        EDMED_JSON["dateCreated"] = value

        value = (self.getValueHTML("Geographical area", datasetContents))
        mapping["spatialCoverage"] = str(value)
        EDMED_JSON["spatialCoverage"] = value

        value = (self.getValueHTML("Parameters", datasetContents))
        mapping["keywords"] = str(value)
        EDMED_JSON["keywords"] = value

        value = (self.getValueHTML("Instruments", datasetContents))
        mapping["measurementTechnique"] = str(value)
        EDMED_JSON["measurementTechnique"] = value

        value = (self.getValueHTML("Summary", datasetContents))
        mapping["description"] = str(value)
        mapping["abstract"] = str(value)
        EDMED_JSON["abstract"] = value

        # mapping["language"]=[LangaugePrediction(TextualContents)]

        value = (self.getValueHTML("Originators", datasetContents))
        mapping["creator"] = str(value)
        EDMED_JSON["creator"] = value

        value = (self.getValueHTML("Data web site", datasetContents))
        mapping["distributionInfo"] = str(value)
        EDMED_JSON["distributionInfo"] = value

        value = (self.getValueHTML("Organisation", datasetContents))
        mapping["publisher"] = str(value)
        EDMED_JSON["publisher"] = value

        value = (self.getValueHTML("Contact", datasetContents))
        mapping["author"] = str(value)
        EDMED_JSON["author"] = value

        value = (self.getValueHTML("Address", datasetContents))
        mapping["contact"] = str(value)
        EDMED_JSON["contact"] = value

        value = (self.getValueHTML("Collating centre", datasetContents))
        mapping["producer"] = str(value)
        mapping["provider"] = str(value)
        EDMED_JSON["provider"] = value

        value = (self.getValueHTML("Local identifier", datasetContents))
        mapping["identifier"] = str(value)
        EDMED_JSON["identifier"] = value

        value = (self.getValueHTML("Last revised", datasetContents))
        mapping["modificationDate"] = str(value)
        EDMED_JSON["modificationDate"] = value

        domains = self.getDomain(RI)
        value = self.topicMining(EDMED_JSON)
        mapping["potentialTopics"] = value

        essentialVariables = self.getDomainEssentialVariables(domains[0])
        value = self.getSimilarEssentialVariables(essentialVariables, value)
        mapping["EssentialVariables"] = value

        for metadata_property in metadataStar_object:
            cnt = cnt + 1
            if cnt == len(metadataStar_object):
                extrachar = "\n"
            else:
                extrachar = ",\n"

            if metadata_property in mapping:
                value = mapping[metadata_property]
                if type(mapping[metadata_property]) != list:
                    value = [value]

                if (
                        metadata_property == "description"
                        or metadata_property == "keywords"
                        or metadata_property == "abstract"):
                    txtVal = (str(value).replace("[", "").replace("]", "")
                              .replace("'", "").replace("\"", "")
                              .replace("\"\"", "")
                              .replace("None", ""))
                    if txtVal != "":
                        originalValues.append(txtVal)

                elif (
                        metadata_property == "potentialTopics"
                        or metadata_property == "EssentialVariables"):
                    value = self.pruneExtractedContextualInformation(
                        value, originalValues)

                indexFile.write(
                    "\""
                    + str(metadata_property)
                    + "\" :"
                    + str(value).replace("'", "\"") + extrachar
                    )
            else:
                indexFile.write(
                    "\""
                    + str(metadata_property)
                    + "\" :"
                    + str([])
                    + extrachar
                    )

        indexFile.write("}")
        indexFile.close()


class SeaDataNetEDMEDRepository(Repository):
    name = 'SeaDataNet EDMED'
    research_infrastructure = 'SeaDatanet'

    downloader = SeaDataNetEDMEDDownloader
    mapper = SeaDataNetEDMEDMapper
    indexer = Indexer
