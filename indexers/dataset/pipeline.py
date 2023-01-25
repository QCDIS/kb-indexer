import shlex
import subprocess
import os
import re
import spacy
import nltk
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
import string
import gensim
from gensim import corpora
import enchant
from fuzzywuzzy import fuzz
import urllib.request
import uuid
import json
from lxml.etree import fromstring
from xml.etree import ElementTree
import glob
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import random

from .. import utils

from .web_crawler import Crawler
from . import synonyms


class LanguageTools:
    def __init__(self):
        nltk_data_dir = os.path.join(
            os.path.dirname(__file__), 'pipeline_io/nltk_data'
            )
        nltk.data.path.append(nltk_data_dir)
        nltk.download('wordnet', download_dir=nltk_data_dir)
        nltk.download('stopwords', download_dir=nltk_data_dir)

        synonyms.download_nltk_dependencies_if_needed(nltk_data_dir)

        self.EnglishTerm = enchant.Dict("en_US")
        self.stop = set(stopwords.words('english'))
        self.exclude = set(string.punctuation)
        self.lemma = WordNetLemmatizer()
        self.spacy_nlp = spacy.load('en_core_web_md')


class DeepSearch:
    def __init__(self):
        self.foundResults = []

    def search(self, needles, haystack):
        found = {}
        if type(needles) != type([]):
            needles = [needles]

        if type(haystack) == type(dict()):
            for needle in needles:
                if needle in haystack.keys():
                    found[needle] = haystack[needle]
                elif len(haystack.keys()) > 0:
                    # Fuzzy calculation
                    for key in haystack.keys():
                        if fuzz.ratio(needle.lower(), key.lower()) > 75:
                            found[needle] = haystack[key]
                            break
                    # (end)
                    for key in haystack.keys():
                        result = self.search(needle, haystack[key])
                        if result:
                            for k, v in result.items():
                                found[k] = v
                                self.foundResults.append(
                                    v
                                    ) if v not in self.foundResults else self.foundResults
        elif type(haystack) == type([]):
            for node in haystack:
                result = self.search(needles, node)
                if result:
                    for k, v in result.items():
                        found[k] = v
                        self.foundResults.append(
                            v
                            ) if v not in self.foundResults else self.foundResults
        return found


class DatasetIndexer:
    acceptedSimilarityThreshold = 0.75
    CommonSubsetThreshold = 0.0

    source_name: str
    MetadataRecordsFileName: str
    contextual_text_fields: list[str]
    contextual_text_fallback_field: str

    def __init__(self):
        self.indexer = utils.ElasticsearchIndexer('envri-test')
        self.lt = LanguageTools()

        cwd = os.path.dirname(__file__)
        self.MetaDataRecordPath = cwd + "/data/Metadata records/"
        self.indexFiles_root = cwd + "/data/index files/"

        os.makedirs(self.indexFiles_root, exist_ok=True)
        os.makedirs(self.MetaDataRecordPath, exist_ok=True)

        self.metadataStar_filename = cwd + "/data_sources/metadata*.json"
        self.RI_filename = cwd + "/data_sources/RIs.json"
        self.domain_filename = cwd + "/data_sources/domain.json"
        self.essentialVariabels_filename = (
                cwd + "/data_sources/essential_variables.json")
        self.domainVocbularies_filename = (
                cwd + "/data_sources/Vocabularies.json")

    def getDatasetRecords(self):
        pass

    def saveSelectedURLs(self, lstDataset, datasetTitle):
        RndIndexFiles = os.path.join(self.indexFiles_root, datasetTitle + ".csv")
        with open(RndIndexFiles, 'w') as f:
            for url in lstDataset:
                f.write("%s\n" % url)
        f.close()

    def getOnlineDatasetRecords(self, rnd, genRnd, startingPoint) -> list[str]:
        pass

    def getContextualText(self, JSON):
        contextualText = DeepSearch().search(self.contextual_text_fields, JSON)
        if not len(contextualText):
            contextualText = DeepSearch().search(
                [self.contextual_text_fallback_field], JSON)
        contextualText = list(NestedDictValues(contextualText))
        return MergeList(contextualText)

    @staticmethod
    def extractTextualContent(y):
        out = {}
        lstvalues = []

        def flatten(x, name=''):
            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '_')
            elif type(x) is list:
                i = 0
                for a in x:
                    flatten(a, name + str(i) + '_')
                    i += 1
            else:
                out[name[:-1]] = x
                text = x
                if type(text) == list or type(text) == dict:
                    text = " ".join(str(x) for x in text)
                if type(text) == str and len(text) > 1:
                    text = re.sub(r'http\S+', '', text)
                    if type(text) == str and len(text) > 1:
                        lstvalues.append(text)

        flatten(y)
        return lstvalues

    def clean(self, doc):
        integer_free = ''.join([i for i in doc if not i.isdigit()])
        stop_free = " ".join(
            [i for i in integer_free.lower().split() if i not in self.lt.stop]
            )
        punc_free = ''.join(ch for ch in stop_free if ch not in self.lt.exclude)
        normalized = " ".join(
            self.lt.lemma.lemmatize(word) for word in punc_free.split() if
            len(word) > 2 and self.lt.EnglishTerm.check(word)
            )
        return normalized

    def topicMining(self, dataset_json):
        #########################################
        # Turn it off:
        #    if RI=="SeaDataNet_EDMED": Jsontext=getContextualText_SeaDataNet_EDMED(dataset_json)
        #    if RI=="SeaDataNet_CDI": Jsontext= getContextualText_SeaDataNet_CDI (dataset_json)
        #    if RI=="LifeWatch": Jsontext= getContextualText_LifeWatch (dataset_json)
        #    if RI=="ICOS": Jsontext= getContextualText_ICOS (dataset_json)
        #    return Jsontext
        ########################################
        lsttopic = []
        if dataset_json != "":
            Jsontext = self.getContextualText(dataset_json)
            if not Jsontext:
                Jsontext = self.extractTextualContent(dataset_json)
            doc_clean = [self.clean(doc).split() for doc in Jsontext]
            dictionary = corpora.Dictionary(doc_clean)
            doc_term_matrix = [dictionary.doc2bow(doc) for doc in doc_clean]
            if len(doc_term_matrix) > 0:
                ldamodel = gensim.models.LdaMulticore(
                    corpus=doc_term_matrix, id2word=dictionary, num_topics=3,
                    passes=10
                    )
                topics = ldamodel.show_topics(log=True, formatted=True)
                topTopics = sum(
                    [re.findall('"([^"]*)"', listToString(t[1])) for t in topics],
                    []
                    )
                for topic in topTopics:
                    lsttopic.append(topic) if topic not in lsttopic else lsttopic
        return lsttopic

    def getTopicsByDomainVocabulareis(self, topics, domain):
        Vocabs = []
        domainVocbularies_content = open(self.domainVocbularies_filename, "r")
        domainVocbularies_object = json.loads(
            r'' + domainVocbularies_content.read()
            )
        for vocab in domainVocbularies_object[domain]:
            for topic in topics:
                w1 = self.lt.spacy_nlp(topic.lower())
                w2 = self.lt.spacy_nlp(vocab.lower())
                similarity = w1.similarity(w2)
                if similarity > self.acceptedSimilarityThreshold:
                    Vocabs.append(vocab) if vocab not in Vocabs else Vocabs
        return Vocabs

    def getSimilarEssentialVariables(self, essentialVariables, topics):
        lstEssentialVariables = []
        lsttopics = [*synonyms.getSynonyms(topics), *topics]
        for variable in essentialVariables:
            for topic in lsttopics:
                w1 = self.lt.spacy_nlp(topic.lower())
                w2 = self.lt.spacy_nlp(variable.lower())
                similarity = w1.similarity(w2)
                if similarity >= self.acceptedSimilarityThreshold:
                    lstEssentialVariables.append(
                        variable
                        ) if variable not in lstEssentialVariables else lstEssentialVariables
        return lstEssentialVariables

    def getDomainEssentialVariables(self, domain):
        essentialVariabels_content = open(self.essentialVariabels_filename, "r")
        essentialVariabels_json = json.loads(
            r'' + essentialVariabels_content.read()
            )
        for domainVar in essentialVariabels_json:
            if domain == domainVar:
                return essentialVariabels_json[domain]

    def getRI(self, dataset_JSON):
        RI_content = open(self.RI_filename, "r")
        RI_json = json.loads(r'' + RI_content.read())
        dataset_content = self.extractTextualContent(dataset_JSON)
        for RI in RI_json:
            for RI_keys in RI_json[RI]:
                for ds in dataset_content:
                    if RI_keys in ds:
                        return RI

    def getDomain(self, RI_seed):
        domain_content = open(self.domain_filename, "r")
        domain_json = json.loads(r'' + domain_content.read())
        for RI in domain_json:
            if RI == RI_seed:
                return domain_json[RI]

    def refineResults(self, TextArray, datatype, proprtyName):
        datatype = datatype.lower()
        refinedResults = []
        if len(TextArray):
            if type(TextArray) == str:
                TextArray = [TextArray]

            if type(TextArray) == dict:
                TextArray = list(NestedDictValues(TextArray))

            if type(TextArray) == list:
                TextArray = flatten_list(TextArray)
                values = []
                for text in TextArray:
                    if type(text) == dict:
                        text = list(NestedDictValues(text))
                        values.append(text)
                    elif type(text) == list:
                        values = values + text
                    else:
                        values.append(text)
                if type(values) == list and len(values):
                    TextArray = flatten_list(values)
            if type(TextArray) == type(None):
                TextArray = ["\"" + str(TextArray) + "\""]

            for text in TextArray:
                doc = self.lt.spacy_nlp(str(text).strip())

                if "url" in datatype and type(text) == str:
                    urls = re.findall(r"(?P<url>https?://\S+)", text)
                    if len(urls):
                        refinedResults.append(
                            urls
                            ) if urls not in refinedResults else refinedResults

                if "person" in datatype:
                    if doc.ents:
                        for ent in doc.ents:
                            if (len(ent.text) > 0) and ent.label_ == "PERSON":
                                refinedResults.append(
                                    ent.text
                                    ) if ent.text not in refinedResults else refinedResults

                if "organization" in datatype:
                    if doc.ents:
                        for ent in doc.ents:
                            if (len(ent.text) > 0) and ent.label_ == "ORG":
                                refinedResults.append(
                                    ent.text
                                    ) if ent.text not in refinedResults else refinedResults

                if "place" in datatype:
                    if doc.ents:
                        for ent in doc.ents:
                            if (len(
                                    ent.text
                                    ) > 0) and ent.label_ == "GPE" or ent.label_ == "LOC":
                                refinedResults.append(
                                    ent.text
                                    ) if ent.text not in refinedResults else refinedResults

                if "date" in datatype:
                    if doc.ents:
                        for ent in doc.ents:
                            if (len(ent.text) > 0) and ent.label_ == "DATE":
                                refinedResults.append(
                                    ent.text
                                    ) if ent.text not in refinedResults else refinedResults

                if "product" in datatype:
                    if doc.ents:
                        for ent in doc.ents:
                            if (len(ent.text) > 0) and ent.label_ == "PRODUCT":
                                refinedResults.append(
                                    ent.text
                                    ) if ent.text not in refinedResults else refinedResults

                if ("integer" in datatype) or ("number" in datatype):
                    if doc.ents:
                        for ent in doc.ents:
                            if (len(ent.text) > 0) and ent.label_ == "CARDINAL":
                                refinedResults.append(
                                    ent.text
                                    ) if ent.text not in refinedResults else refinedResults

                if "money" in datatype:
                    if doc.ents:
                        for ent in doc.ents:
                            if (len(ent.text) > 0) and ent.label_ == "MONEY":
                                refinedResults.append(
                                    ent.text
                                    ) if ent.text not in refinedResults else refinedResults

                if "workofart" in datatype:
                    if doc.ents:
                        for ent in doc.ents:
                            if (len(ent.text) > 0) and ent.label_ == "WORK_OF_ART":
                                refinedResults.append(
                                    ent.text
                                    ) if ent.text not in refinedResults else refinedResults

                if "language" in datatype:
                    if doc.ents:
                        for ent in doc.ents:
                            if (len(
                                    ent.text
                                    ) > 0) and ent.label_ == "LANGUAGE" or ent.label_ == "GPE":
                                refinedResults.append(
                                    ent.text
                                    ) if ent.text not in refinedResults else refinedResults

                if proprtyName.lower() not in str(text).lower() and (
                        "text" in datatype or "definedterm" in datatype):
                    refinedResults.append(
                        text
                        ) if text not in refinedResults else refinedResults

        return refinedResults

    def pruneExtractedContextualInformation(self, drivedValues, originalValues):
        #########################################
        # Turn it off:
        # return drivedValues
        ########################################
        lstAcceptedValues = []
        if len(drivedValues) and len(originalValues):
            for subDrivedField in drivedValues:
                for originalField in originalValues:
                    simScore = get_jaccard_sim(
                        subDrivedField.lower(), originalField.lower()
                        )
                    if (
                            simScore > self.CommonSubsetThreshold and
                            subDrivedField not in lstAcceptedValues):
                        lstAcceptedValues.append(subDrivedField)
        else:
            lstAcceptedValues = drivedValues

        return lstAcceptedValues

    def datasetProcessing(self, datasetURL):
        pass

    def if_URL_exist(self, url):
        return self.indexer.is_in_index('url', url)

    def deleteAllIndexFilesByExtension(self, extension):
        directory = self.indexFiles_root
        files_in_directory = os.listdir(directory)
        filtered_files = [file for file in files_in_directory if
                          file.endswith(extension)]
        for file in filtered_files:
            path_to_file = os.path.join(directory, file)
            os.remove(path_to_file)

    def Run_indexingPipeline_ingest_indexFiles(self):
        """ Create index if not exist with correct settings if index exists,
        change settings
        """
        # path is correct IF this file is in the same folder as 'envri_json'
        indexfnames = os.path.join(self.indexFiles_root)
        filelist = glob.glob(indexfnames + "*")

        indexed = 0
        for i in range(len(filelist)):
            doc = open_file(filelist[i])
            print(
                round(((i + 1) / len(filelist) * 100), 2), "%", filelist[i]
                )  # keep track of progress / counter
            indexed += 1
            id_ = utils.gen_id_from_url(doc['url'])
            self.indexer.ingest_record(id_, doc)
        self.deleteAllIndexFilesByExtension(".json")

    def Run_indexingPipeline(self):
        print(f'indexing the {self.source_name} dataset repository')
        self.getDatasetRecords()

        cnt = 1
        lstDataset = self.getOnlineDatasetRecords(False, 10, 1)
        for datasetURL in lstDataset:
            if not (self.if_URL_exist(datasetURL)):
                self.datasetProcessing(datasetURL)
                progress = cnt / len(lstDataset)
                print(f'\n {self.source_name} ----> \n Record: {progress:.3g} % \n ----> \n')
            else:
                print(datasetURL)
            cnt = cnt + 1

            self.deleteAllIndexFilesByExtension(".csv")
            self.Run_indexingPipeline_ingest_indexFiles()
        print("The indexing process has been finished!")


class SeaDataNetCDIIndexer(DatasetIndexer):
    source_name = 'SeaDataNet CDI'
    MetadataRecordsFileName = "SeaDataNet-CDI-metadata-records.xml"
    contextual_text_fields = [
        "Data set name", "Discipline", "Parameter groups",
        "Discovery parameter", "GEMET-INSPIRE themes"]
    contextual_text_fallback_field = "Abstract"

    def getDatasetRecords(self):
        with urllib.request.urlopen(
                'https://cdi.seadatanet.org/report/aggregation'
                ) as f:
            data = f.read().decode('utf-8')
        indexFile = open(
            self.MetaDataRecordPath + self.MetadataRecordsFileName,
            "w+"
            )
        indexFile.write(data)
        indexFile.close()
        print("SeaDataNet CDI data collection is done!")

    def getOnlineDatasetRecords(self, rnd, genRnd, startingPoint):
        tree = ElementTree.parse(
            self.MetaDataRecordPath + self.MetadataRecordsFileName
            )
        indexFile = tree.getroot()
        cnt = 1
        random_selection = random.sample(
            range(startingPoint, len(indexFile)), genRnd
            )
        c = 0
        lstDatasetCollection = []
        for record in indexFile:
            if cnt in random_selection or not rnd:
                c = c + 1
                url = record.text
                pos = url.rfind("/xml")
                if pos and pos + 4 == len(url):
                    url = url.replace("/xml", "/json")
                lstDatasetCollection.append(url)
            cnt = cnt + 1
        self.saveSelectedURLs(lstDatasetCollection, "SeaDataNet_CDI")
        return lstDatasetCollection

    def datasetProcessing(self, datasetURL):
        metadataStar_content = open(self.metadataStar_filename, "r")
        metadataStar_object = json.loads(r'' + metadataStar_content.read())
        with urllib.request.urlopen(datasetURL) as f:
            data = f.read().decode('utf-8')
        JSON = json.loads(r'' + data)

        unique_filename = str(uuid.uuid4())
        indexfname = os.path.join(
            self.indexFiles_root, "SeaDataNet_CDI_" + unique_filename
            )
        indexFile = open(indexfname + ".json", "w+")

        logfile = os.path.join(self.indexFiles_root, "logfile.csv")
        CSVvalue = ""
        if not os.path.exists(logfile):
            logfile = open(logfile, "a+")
            for metadata_property in metadataStar_object:
                CSVvalue = CSVvalue + metadata_property
            logfile.write(CSVvalue)
        else:
            logfile = open(logfile, "a+")

        CSVvalue = "\n"

        indexFile.write("{\n")
        originalValues = []
        RI = ""
        domains = ""
        topics = []
        cnt = 0
        lstKeywords = []
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
                    result, originalValues
                    )
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
                    result, originalValues
                    )
            elif metadata_property == "url":
                result = datasetURL  # [str(datasetURL)]
            elif metadata_property == "name":
                result = DeepSearch().search(["Data set name"], JSON)
            else:
                result = DeepSearch().search([metadata_property], JSON)
                if not len(result):
                    searchFields = []
                    for i in range(3, len(metadataStar_object[metadata_property])):
                        result = DeepSearch().search(
                            [metadataStar_object[metadata_property][i]], JSON
                            )
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

            flattenValue = (str(MergeList(flatten_list(result)))
                            .replace("></a", "").replace(",", "-")
                            .replace("[", "").replace("]", "").replace("{", "")
                            .replace("'", "").replace("\"", "").replace("}", "")
                            .replace("\"\"", "").replace(">\\", "")
                            .replace("' ", "'").replace(" '", "'"))
            flattenValue = str([x.strip() for x in flattenValue.split('-')])

            if (
                    metadata_property == "description" or metadata_property == "keywords" or metadata_property == "abstract"):
                txtVal = flattenValue.replace("[", "").replace("]", "").replace(
                    "'", ""
                    ).replace("\"", "").replace("\"\"", "").replace("None", "")
                if txtVal != "":
                    originalValues.append(txtVal)

            indexFile.write(
                "\"" + str(metadata_property) + "\" :" + flattenValue.replace(
                    "'", "\""
                    ) + extrachar
                )
            CSVvalue = CSVvalue + flattenValue.replace(",", "-").replace(
                "[", ""
                ).replace(
                "]", ""
                ).replace("'", "").replace("\"", "").replace("\"\"", "") + ","

            if metadata_property == "keywords":
                lstKeywords = flattenValue

        indexFile.write("}")
        indexFile.close()

        logfile.write(CSVvalue)
        logfile.close()


class SeaDataNetEDMEDIndexer(DatasetIndexer):
    source_name = 'SeaDataNet EDMED'
    MetadataRecordsFileName = "SeaDataNet-EDMED-metadata-records.json"
    contextual_text_fields = ["name", "keywords", "measurementTechnique"]
    contextual_text_fallback_field = "abstract"

    def __init__(self):
        super().__init__()
        self.lstCoveredFeaturesSeaDataNet = []

    def getDatasetRecords(self):
        with urllib.request.urlopen(
                'https://edmed.seadatanet.org/sparql/sparql?query=select+%3FEDMEDRecord+%3FTitle+where+%7B%3FEDMEDRecord+a+%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fdcat%23Dataset%3E+%3B+%3Chttp%3A%2F%2Fpurl.org%2Fdc%2Fterms%2Ftitle%3E+%3FTitle+.%7D+&output=json&stylesheet='
                ) as f:
            data = f.read().decode('utf-8')
        json_data = json.loads(r'' + data)
        indexFile = open(
            self.MetaDataRecordPath + self.MetadataRecordsFileName, "w+"
            )
        indexFile.write(json.dumps(json_data))
        indexFile.close()
        print("SeaDataNet EDMED data collection is done!")

    def getOnlineDatasetRecords(self, rnd, genRnd, startingPoint):
        indexFile = open(
            self.MetaDataRecordPath + self.MetadataRecordsFileName, "r"
            )
        dataset_json = json.loads(r'' + indexFile.read())
        cnt = 1
        random_selection = random.sample(
            range(startingPoint, len(dataset_json["results"]["bindings"])), genRnd
            )
        c = 0
        lstDatasetCollection = []

        for record in dataset_json["results"]["bindings"]:
            if cnt in random_selection or not rnd:
                c = c + 1
                landingPage = record["EDMEDRecord"]["value"]
                title = record["Title"]["value"]
                lstDatasetCollection.append(landingPage)
            cnt = cnt + 1
        self.saveSelectedURLs(lstDatasetCollection, "SeaDataNet_EDMED")
        return lstDatasetCollection

    @staticmethod
    def _cleanhtml(raw_html):
        CLEANR = re.compile('<.*?>')
        cleantext = re.sub(CLEANR, '', raw_html)
        return (''.join(x for x in cleantext if x in string.printable)).replace(
            "'", ""
            ).replace("\"", "").strip()

    def getValueHTML(self, searchTerm, datasetContents):
        for datasetContent in datasetContents:
            datasetContent = str(datasetContent)
            if searchTerm in datasetContent and searchTerm not in self.lstCoveredFeaturesSeaDataNet:
                self.lstCoveredFeaturesSeaDataNet.append(searchTerm)
                return self._cleanhtml(datasetContent)[len(searchTerm):]

    def datasetProcessing(self, datasetURL):
        metadataStar_content = open(self.metadataStar_filename, "r")
        metadataStar_object = json.loads(r'' + metadataStar_content.read())
        unique_filename = str(uuid.uuid4())
        indexfname = os.path.join(
            self.indexFiles_root, "SeaDataNet_EDMED_" + unique_filename
            )
        indexFile = open(indexfname + ".json", "w+")
        logfile = os.path.join(self.indexFiles_root, "logfile.csv")
        CSVvalue = ""
        originalValues = []
        if not os.path.exists(logfile):
            logfile = open(logfile, "a+")
            for metadata_property in metadataStar_object:
                CSVvalue = CSVvalue + metadata_property
            logfile.write(CSVvalue)
        else:
            logfile = open(logfile, "a+")
        EDMED_JSON = {}
        indexFile.write("{\n")
        datasetContents = Crawler().getHTMLContent(datasetURL, "tr")
        self.lstCoveredFeaturesSeaDataNet.clear()
        mapping = {}
        cnt = 0
        lstKeywords = []
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
        if type(value) == str or type(value) == list:
            lstKeywords = str(value)

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

        CSVvalue = "\n"

        for metadata_property in metadataStar_object:
            cnt = cnt + 1
            if (cnt == len(metadataStar_object)):
                extrachar = "\n"
            else:
                extrachar = ",\n"

            if metadata_property in mapping:
                value = mapping[metadata_property]
                if type(mapping[metadata_property]) != list:
                    value = [value]

                if (
                        metadata_property == "description" or metadata_property == "keywords" or metadata_property == "abstract"):
                    txtVal = str(value).replace("[", "").replace("]", "").replace(
                        "'", ""
                        ).replace("\"", "").replace("\"\"", "").replace("None", "")
                    if txtVal != "":
                        originalValues.append(txtVal)

                elif metadata_property == "potentialTopics" or metadata_property == "EssentialVariables":
                    value = self.pruneExtractedContextualInformation(
                        value, originalValues
                        )

                indexFile.write(
                    "\"" + str(metadata_property) + "\" :" + str(value).replace(
                        "'", "\""
                        ) + extrachar
                    )
                CSVvalue = CSVvalue + str(value).replace(",", "-").replace(
                    "[", ""
                    ).replace(
                    "]", ""
                    ).replace("'", "").replace("\"", "") + ","
            else:
                indexFile.write(
                    "\"" + str(metadata_property) + "\" :" + str([]) + extrachar
                    )
                CSVvalue = CSVvalue + ","

        #    value=(self.getValueHTML_SeaDataNet("Availability", datasetContents))
        #    value=(self.getValueHTML_SeaDataNet("Ongoing", datasetContents))
        #    value=(self.getValueHTML_SeaDataNet("Global identifier", datasetContents))
        indexFile.write("}")
        indexFile.close()

        logfile.write(CSVvalue)
        logfile.close()


class ICOSIndexer(DatasetIndexer):
    source_name = 'ICOS'
    MetadataRecordsFileName = "ICOS-metadata-records.json"
    contextual_text_fields = ["keywords", "genre", "theme", "name"]
    contextual_text_fallback_field = "Abstract"

    def getDatasetRecords(self):
        cURL = r"""curl https://meta.icos-cp.eu/sparql -H 'Cache-Control: no-cache' -X POST --data 'query=prefix%20cpmeta%3A%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fontologies%2Fcpmeta%2F%3E%0Aprefix%20prov%3A%20%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fprov%23%3E%0Aselect%20%3Fdobj%20%3Fspec%20%3FfileName%20%3Fsize%20%3FsubmTime%20%3FtimeStart%20%3FtimeEnd%0Awhere%20%7B%0A%09VALUES%20%3Fspec%20%7B%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FradonFluxSpatialL3%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2Fco2EmissionInventory%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FsunInducedFluorescence%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FoceanPco2CarbonFluxMaps%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FinversionModelingSpatial%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FbiosphereModelingSpatial%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FecoFluxesDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FecoEcoDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FecoMeteoDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FecoAirTempMultiLevelsDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FecoProfileMultiLevelsDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcMeteoL0DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcLosGatosL0DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcPicarroL0DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FingosInversionResult%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2Fsocat_DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcBioMeteoRawSeriesBin%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcStorageFluxRawSeriesBin%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcBioMeteoRawSeriesCsv%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcStorageFluxRawSeriesCsv%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcSaheatFlagFile%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FceptometerMeasurements%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FglobalCarbonBudget%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FnationalCarbonEmissions%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FglobalMethaneBudget%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FdigHemispherPics%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcEddyFluxRawSeriesCsv%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcEddyFluxRawSeriesBin%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCh4L2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCoL2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCo2L2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcMtoL2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcC14L2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcMeteoGrowingNrtDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCo2NrtGrowingDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCh4NrtGrowingDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcN2oL2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCoNrtGrowingDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcN2oNrtGrowingDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FingosCh4Release%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FingosN2oRelease%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcRnNrtDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2Fdrought2018AtmoProduct%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FmodelDataArchive%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcArchiveProduct%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2Fdought2018ArchiveProduct%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatmoMeasResultsArchive%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcNrtAuxData%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcFluxnetProduct%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2Fdrought2018FluxnetProduct%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcNrtFluxes%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcNrtMeteosens%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcNrtMeteo%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FicosOtcL1Product%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FicosOtcL1Product_v2%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FicosOtcL2Product%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FicosOtcFosL2Product%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FotcL0DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FinversionModelingTimeseries%3E%7D%0A%09%3Fdobj%20cpmeta%3AhasObjectSpec%20%3Fspec%20.%0A%09%3Fdobj%20cpmeta%3AhasSizeInBytes%20%3Fsize%20.%0A%3Fdobj%20cpmeta%3AhasName%20%3FfileName%20.%0A%3Fdobj%20cpmeta%3AwasSubmittedBy%2Fprov%3AendedAtTime%20%3FsubmTime%20.%0A%3Fdobj%20cpmeta%3AhasStartTime%20%7C%20%28cpmeta%3AwasAcquiredBy%20%2F%20prov%3AstartedAtTime%29%20%3FtimeStart%20.%0A%3Fdobj%20cpmeta%3AhasEndTime%20%7C%20%28cpmeta%3AwasAcquiredBy%20%2F%20prov%3AendedAtTime%29%20%3FtimeEnd%20.%0A%09FILTER%20NOT%20EXISTS%20%7B%5B%5D%20cpmeta%3AisNextVersionOf%20%3Fdobj%7D%0A%7D%0Aorder%20by%20desc%28%3FsubmTime%29'"""
        lCmd = shlex.split(cURL)  # Splits cURL into an array
        p = subprocess.Popen(lCmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()  # Get the output and the err message
        json_data = json.loads(r'' + out.decode("utf-8"))
        indexFile = open(self.MetaDataRecordPath +
                         self.MetadataRecordsFileName, "w+")
        indexFile.write(json.dumps(json_data))
        indexFile.close()
        print("ICOS data collection is done!")

    def getOnlineDatasetRecords(self, rnd, genRnd, startingPoint):
        indexFile = open(self.MetaDataRecordPath +
                         self.MetadataRecordsFileName, "r")
        dataset_json = json.loads(r'' + indexFile.read())

        cnt = 1
        random_selection = random.sample(
            range(startingPoint, len(dataset_json["results"]["bindings"])), genRnd
            )
        c = 0

        lstDatasetCollection = []

        for record in dataset_json["results"]["bindings"]:
            if cnt in random_selection or not rnd:
                c = c + 1
                filename = record["fileName"]["value"]
                landingPage = record["dobj"]["value"]
                timeEnd = record["timeEnd"]["value"]
                timeStart = record["timeStart"]["value"]
                submTime = record["submTime"]["value"]
                size = record["size"]["value"]
                spec = record["spec"]["value"]
                # downloadableLink= "https://data.icos-cp.eu/objects/"+os.path.basename(urlparse(landingPage).path)

                lstDatasetCollection.append(landingPage)
            cnt = cnt + 1
        self.saveSelectedURLs(lstDatasetCollection, "ICOS")
        return lstDatasetCollection

    def datasetProcessing(self, datasetURL):
        metadataStar_content = open(self.metadataStar_filename, "r")
        metadataStar_object = json.loads(r'' + metadataStar_content.read())
        unique_filename = str(uuid.uuid4())
        indexfname = os.path.join(self.indexFiles_root, "ICOS_" + unique_filename)
        indexFile = open(indexfname + ".json", "w+")
        essentialVariables = []
        logfile = os.path.join(self.indexFiles_root, "logfile.csv")
        CSVvalue = ""
        if not os.path.exists(logfile):
            logfile = open(logfile, "a+")
            for metadata_property in metadataStar_object:
                CSVvalue = CSVvalue + metadata_property
            logfile.write(CSVvalue)
        else:
            logfile = open(logfile, "a+")

        CSVvalue = "\n"

        indexFile.write("{\n")

        scripts = Crawler().getHTMLContent(datasetURL, "script")
        for script in scripts:
            if '<script type="application/ld+json">' in str(script):
                script = str(script)
                start = script.find('<script type="application/ld+json">') + len(
                    '<script type="application/ld+json">'
                    )
                end = script.find("</script>")
                script = script[start:end]
                JSON = json.loads(r'' + script)
                RI = ""
                domains = ""
                topics = []
                originalValues = []
                cnt = 0
                lstKeywords = []
                for metadata_property in metadataStar_object:
                    cnt = cnt + 1

                    if metadata_property == "ResearchInfrastructure":
                        result = self.getRI(JSON)
                    elif metadata_property == "theme":
                        if not len(RI):
                            # RI= self.getRI(JSON)
                            RI = "ICOS"
                        if not len(domains):
                            domains = self.getDomain(RI)
                        if not len(topics):
                            topics = self.topicMining(JSON)
                        result = self.getTopicsByDomainVocabulareis(topics, domains[0])
                    elif metadata_property == "potentialTopics":
                        if not len(topics):
                            topics = self.topicMining(JSON)
                        result = topics
                        result = self.pruneExtractedContextualInformation(
                            result, originalValues
                            )
                    elif metadata_property == "EssentialVariables":
                        if not len(RI):
                            RI = self.getRI(JSON)
                        if not len(domains):
                            domains = self.getDomain(RI)
                        if not len(topics):
                            topics = self.topicMining(JSON)
                        essentialVariables = self.getDomainEssentialVariables(
                            domains[0]
                            )
                        result = self.getSimilarEssentialVariables(
                            essentialVariables, topics
                            )
                        result = self.pruneExtractedContextualInformation(
                            result, originalValues
                            )
                    elif metadata_property == "url":
                        result = datasetURL  # [str(datasetURL)]
                    else:
                        result = DeepSearch().search([metadata_property], JSON)
                        if not len(result):
                            searchFields = []
                            for i in range(
                                    3, len(
                                        metadataStar_object[metadata_property]
                                        )
                                    ):
                                result = DeepSearch().search(
                                    [metadataStar_object[metadata_property][i]],
                                    JSON
                                    )
                                if len(result):
                                    searchFields.append(result)
                            result = searchFields
                    propertyDatatype = metadataStar_object[metadata_property][0]
                    # if metadata_property!="url":
                    result = self.refineResults(
                        result, propertyDatatype, metadata_property
                        )

                    # if metadata_property=="language" and (result=="" or result==[]):
                    #   result= LangaugePrediction(self.extractTextualContent(JSON))

                    if cnt == len(metadataStar_object):
                        extrachar = "\n"
                    else:
                        extrachar = ",\n"

                    flattenValue = str(flatten_list(result))
                    if flattenValue == "[None]":
                        flattenValue = "[]"

                    if (
                            metadata_property == "description" or metadata_property == "keywords" or metadata_property == "abstract"):
                        txtVal = flattenValue.replace("[", "").replace(
                            "]", ""
                            ).replace(
                            "'", ""
                            ).replace("\"", "").replace("\"\"", "").replace(
                            "None", ""
                            )
                        if txtVal != "":
                            originalValues.append(txtVal)

                    if metadata_property == "landingPage":
                        flattenValue = "[]"

                    indexFile.write(
                        "\"" + str(metadata_property) + "\" :" +
                        flattenValue.replace("']", "\"]").replace(
                            "['", "[\""
                            ).replace(
                            "',", "\","
                            ).replace(
                            ", '", ", \""
                            ).replace("\"\"", "\"")
                        + extrachar
                        )
                    CSVvalue = CSVvalue + flattenValue.replace(",", "-").replace(
                        "[", ""
                        ).replace("]", "").replace("'", "").replace("\"", "") + ","

                    if metadata_property == "keywords":
                        lstKeywords = flattenValue

        indexFile.write("}")
        indexFile.close()

        logfile.write(CSVvalue)
        logfile.close()


class LifeWatchIndexer(DatasetIndexer):
    source_name = 'LifeWatch'
    MetadataRecordsFileName = "LifeWatch.txt"
    contextual_text_fields = [
        "dataset", "title", "abstract", "citation", "headline", "publisher"]
    contextual_text_fallback_field = "Abstract"

    def getDatasetRecords(self):
        driver = webdriver.Chrome(ChromeDriverManager().install())
        driver.get(
            "https://metadatacatalogue.lifewatch.eu/srv/eng/catalog.search#/search?facet.q=type%2Fdataset&resultType=details&sortBy=relevance&from=301&to=400&fast=index&_content_type=json"
            )
        #    elem = driver.find_element_by_name("q")
        print(print(driver.title))
        print("Lifewatch data collection is done!")
        driver.close()

    def getOnlineDatasetRecords(self, rnd, genRnd, startingPoint):
        indexFile = os.path.join(
            self.MetaDataRecordPath + self.MetadataRecordsFileName
            )
        cnt = 1
        c = 0
        lstDatasetCollection = []
        with open(indexFile) as f:
            lines = f.readlines()
        random_selection = random.sample(range(startingPoint, len(lines)), genRnd)
        baseline = "https://metadatacatalogue.lifewatch.eu/srv/api/records/"
        xml = "/formatters/xml?approved=true"
        for line in lines:
            if cnt in random_selection or not rnd:
                datasetID = (line.split(",")[1].replace("\"", ""))
                if not "oai:marineinfo.org:id:dataset" in datasetID:
                    continue
                c = c + 1
                url = baseline
                lstDatasetCollection.append(url + datasetID + xml)
                cnt = cnt + 1
        self.saveSelectedURLs(lstDatasetCollection, "LifeWatch")
        return lstDatasetCollection

    def datasetProcessing(self, datasetURL):
        metadataStar_content = open(self.metadataStar_filename, "r")
        metadataStar_object = json.loads(r'' + metadataStar_content.read())

        unique_filename = str(uuid.uuid4())
        indexfname = os.path.join(self.indexFiles_root, "LifeWatch_" + unique_filename)
        indexFile = open(indexfname + ".json", "w+")

        logfile = os.path.join(self.indexFiles_root, "logfile.csv")
        CSVvalue = ""
        if not os.path.exists(logfile):
            logfile = open(logfile, "a+")
            for metadata_property in metadataStar_object:
                CSVvalue = CSVvalue + metadata_property
            logfile.write(CSVvalue)
        else:
            logfile = open(logfile, "a+")

        CSVvalue = "\n"

        indexFile.write("{\n")
        originalValues = []
        RI = ""
        domains = ""
        topics = []
        cnt = 0
        lstKeywords = []
        datasetDic = {}
        with urllib.request.urlopen(datasetURL) as f:
            data = f.read().decode('utf-8')
        xmlTree = fromstring(data.encode())
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
                for elem in elemList:
                    datasetDic[elem].remove('')
                    datasetDic[elem].append(strValue)
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
                    result, originalValues
                    )
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
                    result, originalValues
                    )
            elif metadata_property == "url":
                result = datasetURL  # [str(datasetURL)]
            else:
                result = DeepSearch().search([metadata_property], JSON)
                if not len(result):
                    searchFields = []
                    for i in range(3, len(metadataStar_object[metadata_property])):
                        result = DeepSearch().search(
                            [metadataStar_object[metadata_property][i]], JSON
                            )
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

            flattenValue = (str(MergeList(flatten_list(result)))
                            .replace("></a", "").replace(",", "-")
                            .replace("[", "").replace("]", "").replace("{", "")
                            .replace("'", "").replace("\"", "").replace("}", "")
                            .replace("\"\"", "").replace(">\\", "")
                            .replace("' ", "'").replace(" '", "'"))
            flattenValue = str([x.strip() for x in flattenValue.split('-')])

            if (
                    metadata_property == "description" or metadata_property == "keywords" or metadata_property == "abstract"):
                txtVal = flattenValue.replace("[", "").replace("]", "").replace(
                    "'", ""
                    ).replace("\"", "").replace("\"\"", "").replace("None", "")
                if txtVal != "":
                    originalValues.append(txtVal)

            indexFile.write(
                "\"" + str(metadata_property) + "\" :" + flattenValue.replace(
                    "'", "\""
                    ) + extrachar
                )
            CSVvalue = CSVvalue + flattenValue.replace(",", "-").replace(
                "[", ""
                ).replace(
                "]", ""
                ).replace("'", "").replace("\"", "").replace("\"\"", "") + ","

            if metadata_property == "keywords":
                lstKeywords = flattenValue

        indexFile.write("}")
        indexFile.close()

        logfile.write(CSVvalue)
        logfile.close()


def listToString(s):
    str1 = ""
    for ele in s:
        str1 += ele
    return str1


def NestedDictValues(d):
    if type(d) == dict:
        for v in d.values():
            if isinstance(v, dict):
                yield from NestedDictValues(v)
            else:
                yield v


def is_nested_list(l):
    try:
        next(x for x in l if isinstance(x, list))
    except StopIteration:
        return False
    return True


def flatten_list(t):
    if is_nested_list(t):
        return [str(item) for sublist in t for item in sublist if
                not type(sublist) == str]
    return t


def MergeList(contextualText):
    lstText = []
    for entity in contextualText:
        if type(entity) == list:
            for item in entity:
                lstText.append(str(item).strip())
        else:
            lstText.append(str(entity).strip())
    return lstText


def get_jaccard_sim(str1, str2):
    a = set(str1.split())
    b = set(str2.split())
    c = a.intersection(b)
    if (len(a) + len(b) - len(c)) > 0:
        sim = float(len(c)) / (len(a) + len(b) - len(c))
    else:
        sim = 0
    return sim


def open_file(file):
    read_path = file
    with open(read_path, "r", errors='ignore') as read_file:
        data = json.load(read_file)
    return data


def main():
    SeaDataNetCDIIndexer().Run_indexingPipeline()
    SeaDataNetEDMEDIndexer().Run_indexingPipeline()
    ICOSIndexer().Run_indexingPipeline()
    # LifeWatchIndexer().Run_indexingPipeline()


if __name__ == '__main__':
    main()
