import json
import os
import urllib.error
import urllib.request
from xml.etree import ElementTree

from ... import utils
from .common import Repository
from ..download import Downloader
from ..map import Mapper, DeepSearch, flatten_list, merge_list
from ..index import Indexer


class SeaDataNetCDIDownloader(Downloader):
    dataset_list_url = 'https://cdi.seadatanet.org/report/aggregation'
    dataset_list_ext = ".xml"

    def convert_dataset_list_to_dataset_urls(self):
        tree = ElementTree.parse(self.paths.dataset_list_filename)
        indexFile = tree.getroot()
        urls = []
        for record in indexFile:
            url = record.text
            pos = url.rfind("/xml")
            if pos and pos + 4 == len(url):
                url = url.replace("/xml", "/json")
            urls.append(url)
        with open(self.paths.dataset_urls_filename, 'w') as f:
            f.write('\n'.join(urls))


class SeaDataNetCDIMapper(Mapper):
    contextual_text_fields = [
        "Data set name", "Discipline", "Parameter groups",
        "Discovery parameter", "GEMET-INSPIRE themes"]
    contextual_text_fallback_field = "Abstract"

    def gen_record_from_url(self, datasetURL):
        try:
            with urllib.request.urlopen(datasetURL) as f:
                data = f.read().decode('utf-8')
        except urllib.error.HTTPError:
            print(f'Could not open {datasetURL}, skipping')
            return
        JSON = json.loads(r'' + data)

        with open(self.paths.metadataStar_filename, "r") as f:
            metadataStar_object = json.loads(f.read())

        indexfname = os.path.join(
            self.paths.index_records_dir,
            utils.gen_id_from_url(datasetURL) + '.json',
            )
        indexFile = open(indexfname, "w")
        indexFile.write("{\n")

        originalValues = []
        RI = ""
        domains = ""
        topics = []
        cnt = 0
        for metadata_property in metadataStar_object:
            cnt = cnt + 1

            if metadata_property == "ResearchInfrastructure":
                result = self.getRI(JSON)
            elif metadata_property == "theme":
                if not len(RI):
                    # RI= self.getRI(JSON)
                    RI = "SeaDataNet"
                if not len(domains):
                    domains = self.getDomain(RI)
                if not len(topics):
                    topics = self.topicMining(JSON)
                result = self.getTopicsByDomainVocabulareis(topics, domains[0])
            elif metadata_property == "language":
                result = "English"
            elif metadata_property == "potentialTopics":
                if not len(topics):
                    topics = self.topicMining(JSON)
                result = topics
                result = self.pruneExtractedContextualInformation(
                    result, originalValues)
            elif metadata_property == "EssentialVariables":
                if not len(RI):
                    RI = self.getRI(JSON)
                if not len(domains):
                    domains = self.getDomain(RI)
                if not len(topics):
                    topics = self.topicMining(JSON)
                essentialVariables = self.getDomainEssentialVariables(
                    domains[0])
                result = self.getSimilarEssentialVariables(
                    essentialVariables, topics)
                result = self.pruneExtractedContextualInformation(
                    result, originalValues)
            elif metadata_property == "url":
                result = datasetURL  # [str(datasetURL)]
            elif metadata_property == "name":
                result = DeepSearch().search(["Data set name"], JSON)
            else:
                result = DeepSearch().search([metadata_property], JSON)
                if not len(result):
                    searchFields = []
                    m = metadataStar_object[metadata_property]
                    for i in range(3, len(m)):
                        result = DeepSearch().search([m[i]], JSON)
                        if len(result):
                            searchFields.append(result)
                    result = searchFields
            propertyDatatype = metadataStar_object[metadata_property][0]
            # if metadata_property!="url":
            result = self.refineResults(
                result, propertyDatatype, metadata_property)

            # if metadata_property=="language" and (result=="" or result==[]):
            #   result= LangaugePrediction(self.extractTextualContent(JSON))

            if cnt == len(metadataStar_object):
                extrachar = "\n"
            else:
                extrachar = ",\n"

            flattenValue = (str(merge_list(flatten_list(result)))
                            .replace("></a", "").replace(",", "-")
                            .replace("[", "").replace("]", "").replace("{", "")
                            .replace("'", "").replace("\"", "")
                            .replace("}", "")
                            .replace("\"\"", "").replace(">\\", "")
                            .replace("' ", "'").replace(" '", "'"))
            flattenValue = str([x.strip() for x in flattenValue.split('-')])

            if (
                    metadata_property == "description"
                    or metadata_property == "keywords"
                    or metadata_property == "abstract"):
                txtVal = (flattenValue.replace("[", "")
                          .replace("]", "")
                          .replace("'", "")
                          .replace("\"", "")
                          .replace("\"\"", "")
                          .replace("None", ""))
                if txtVal != "":
                    originalValues.append(txtVal)

            indexFile.write(
                "\""
                + str(metadata_property)
                + "\" :"
                + flattenValue.replace("'", "\"")
                + extrachar
                )

        indexFile.write("}")
        indexFile.close()


class SeaDataNetCDIRepository(Repository):
    name = 'SeaDataNet CDI'
    research_infrastructure = 'SeaDatanet'

    downloader = SeaDataNetCDIDownloader
    mapper = SeaDataNetCDIMapper
    indexer = Indexer
