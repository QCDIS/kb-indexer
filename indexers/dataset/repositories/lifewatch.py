import json
import os
import urllib.error
import urllib.request

from lxml.etree import fromstring
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

from ... import utils
from .common import Repository
from ..download import Downloader
from ..map import Mapper, DeepSearch, flatten_list, merge_list
from ..index import Indexer


class LifeWatchDownloader(Downloader):
    dataset_list_ext = ".txt"

    def get_dataset_list(self):
        driver = webdriver.Chrome(ChromeDriverManager().install())
        driver.get(
            "https://metadatacatalogue.lifewatch.eu/srv/eng/catalog.search#/search?facet.q=type%2Fdataset&resultType=details&sortBy=relevance&from=301&to=400&fast=index&_content_type=json"
            )
        #    elem = driver.find_element_by_name("q")
        print(print(driver.title))
        print("Lifewatch data collection is done!")
        driver.close()

    def convert_dataset_list_to_dataset_urls(self):
        indexFile = os.path.join(self.paths.dataset_list_filename)
        urls = []
        with open(indexFile) as f:
            lines = f.readlines()
        baseline = "https://metadatacatalogue.lifewatch.eu/srv/api/records/"
        xml = "/formatters/xml?approved=true"
        for line in lines:
            datasetID = (line.split(",")[1].replace("\"", ""))
            if "oai:marineinfo.org:id:dataset" not in datasetID:
                continue
            url = baseline
            urls.append(url + datasetID + xml)
        with open(self.paths.dataset_urls_filename, 'w') as f:
            f.write('\n'.join(urls))


class LifeWatchMapper(Mapper):
    contextual_text_fields = [
        "dataset", "title", "abstract", "citation", "headline", "publisher"]
    contextual_text_fallback_field = "Abstract"

    def gen_record_from_url(self, datasetURL):
        try:
            with urllib.request.urlopen(datasetURL) as f:
                data = f.read().decode('utf-8')
            xmlTree = fromstring(data.encode())
        except urllib.error.HTTPError:
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
        RI = ""
        domains = ""
        topics = []
        cnt = 0
        datasetDic = {}
        elemList = []

        for elem in xmlTree.iter():
            strValue = str(elem.text).replace("\n", "").strip()
            strKey = elem.tag

            if strKey not in datasetDic:
                datasetDic[strKey] = [strValue]
            else:
                if strValue not in datasetDic[strKey]:
                    datasetDic[strKey].append(strValue)

            if strValue == "":
                elemList.append(strKey)
            else:
                for k in elemList:
                    datasetDic[k].remove('')
                    datasetDic[k].append(strValue)
                elemList.clear()
        JSON = datasetDic
        for metadata_property in metadataStar_object:
            cnt = cnt + 1

            if metadata_property == "ResearchInfrastructure":
                result = "LifeWatch"
            elif metadata_property == "theme":
                if not len(RI):
                    # RI= self.getRI(JSON)
                    RI = "LifeWatch"
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


class LifeWatchRepository(Repository):
    name = 'LifeWatch'
    research_infrastructure = 'LifeWatch'

    downloader = LifeWatchDownloader
    mapper = LifeWatchMapper
    indexer = Indexer
