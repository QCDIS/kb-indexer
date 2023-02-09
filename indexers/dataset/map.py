import abc
import json
import os
import re
import string

from bs4 import BeautifulSoup
import enchant
from fuzzywuzzy import fuzz
import gensim
import nltk
import requests
import spacy

from .. import utils
from .common import Paths


class Mapper(abc.ABC):
    contextual_text_fields: list[str]
    contextual_text_fallback_field: str

    acceptedSimilarityThreshold = 0.75
    CommonSubsetThreshold = 0.0

    def __init__(self, paths: Paths):
        self.paths = paths
        self.lt = LanguageTools(paths)

    def getContextualText(self, JSON):
        contextualText = DeepSearch().search(self.contextual_text_fields,
                                             JSON)
        if not len(contextualText):
            contextualText = DeepSearch().search(
                [self.contextual_text_fallback_field], JSON)
        contextualText = list(nested_dict_value(contextualText))
        return merge_list(contextualText)

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
        punc_free = ''.join(
            ch for ch in stop_free if ch not in self.lt.exclude
            )
        normalized = " ".join(
            self.lt.lemma.lemmatize(word)
            for word in punc_free.split()
            if len(word) > 2 and self.lt.EnglishTerm.check(word)
            )
        return normalized

    def topicMining(self, dataset_json):
        #########################################
        # Turn it off:
        #    return self.getContextualText(dataset_json)
        ########################################
        lsttopic = []
        if dataset_json != "":
            Jsontext = self.getContextualText(dataset_json)
            if not Jsontext:
                Jsontext = self.extractTextualContent(dataset_json)
            doc_clean = [self.clean(doc).split() for doc in Jsontext]
            dictionary = gensim.corpora.Dictionary(doc_clean)
            doc_term_matrix = [dictionary.doc2bow(doc) for doc in doc_clean]
            if len(doc_term_matrix) > 0:
                ldamodel = gensim.models.LdaMulticore(
                    corpus=doc_term_matrix,
                    id2word=dictionary,
                    num_topics=3,
                    passes=10,
                    )
                topics = ldamodel.show_topics(log=True, formatted=True)
                topTopics = sum(
                    [re.findall('"([^"]*)"', ''.join(t[1]))
                     for t in topics],
                    []
                    )
                for topic in topTopics:
                    if topic not in lsttopic:
                        lsttopic.append(topic)
        return lsttopic

    def getTopicsByDomainVocabulareis(self, topics, domain):
        Vocabs = []
        with open(self.paths.domainVocbularies_filename) as f:
            domainVocbularies_object = json.load(f)
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
        lsttopics = [*self.lt.get_words_synonyms(topics), *topics]
        for variable in essentialVariables:
            for topic in lsttopics:
                w1 = self.lt.spacy_nlp(topic.lower())
                w2 = self.lt.spacy_nlp(variable.lower())
                similarity = w1.similarity(w2)
                if similarity >= self.acceptedSimilarityThreshold:
                    if variable not in lstEssentialVariables:
                        lstEssentialVariables.append(variable)
        return lstEssentialVariables

    def getDomainEssentialVariables(self, domain):
        with open(self.paths.essentialVariabels_filename) as f:
            essentialVariabels_json = json.load(f)
        for domainVar in essentialVariabels_json:
            if domain == domainVar:
                return essentialVariabels_json[domain]

    def getRI(self, dataset_JSON):
        RI_content = open(self.paths.RI_filename, "r")
        RI_json = json.loads(r'' + RI_content.read())
        dataset_content = self.extractTextualContent(dataset_JSON)
        for RI in RI_json:
            for RI_keys in RI_json[RI]:
                for ds in dataset_content:
                    if RI_keys in ds:
                        return RI

    def getDomain(self, RI_seed):
        domain_content = open(self.paths.domain_filename, "r")
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
                TextArray = list(nested_dict_value(TextArray))

            if type(TextArray) == list:
                TextArray = flatten_list(TextArray)
                values = []
                for text in TextArray:
                    if type(text) == dict:
                        text = list(nested_dict_value(text))
                        values.append(text)
                    elif type(text) == list:
                        values = values + text
                    else:
                        values.append(text)
                if isinstance(values, list) and len(values):
                    TextArray = flatten_list(values)
            if TextArray is None:
                TextArray = ["\"" + str(TextArray) + "\""]

            for text in TextArray:
                doc = self.lt.spacy_nlp(str(text).strip())

                if "url" in datatype and type(text) == str:
                    urls = re.findall(r"(?P<url>https?://\S+)", text)
                    if len(urls):
                        if urls not in refinedResults:
                            refinedResults.append(urls)

                if "person" in datatype:
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and ent.label_ == "PERSON":
                                if ent.text not in refinedResults:
                                    refinedResults.append(ent.text)

                if "organization" in datatype:
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and ent.label_ == "ORG":
                                if ent.text not in refinedResults:
                                    refinedResults.append(ent.text)

                if "place" in datatype:
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and (ent.label_ == "GPE" or
                                             ent.label_ == "LOC"):
                                if ent.text not in refinedResults:
                                    refinedResults.append(ent.text)

                if "date" in datatype:
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and ent.label_ == "DATE":
                                if ent.text not in refinedResults:
                                    refinedResults.append(ent.text)

                if "product" in datatype:
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and ent.label_ == "PRODUCT":
                                if ent.text not in refinedResults:
                                    refinedResults.append(ent.text)

                if ("integer" in datatype) or ("number" in datatype):
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and ent.label_ == "CARDINAL":
                                if ent.text not in refinedResults:
                                    refinedResults.append(ent.text)

                if "money" in datatype:
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and ent.label_ == "MONEY":
                                if ent.text not in refinedResults:
                                    refinedResults.append(ent.text)

                if "workofart" in datatype:
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and ent.label_ == "WORK_OF_ART":
                                if ent.text not in refinedResults:
                                    refinedResults.append(ent.text)

                if "language" in datatype:
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and (ent.label_ == "LANGUAGE" or
                                             ent.label_ == "GPE"):
                                if ent.text not in refinedResults:
                                    refinedResults.append(ent.text)

                if proprtyName.lower() not in str(text).lower() and (
                        "text" in datatype or "definedterm" in datatype):
                    if text not in refinedResults:
                        refinedResults.append(text)

        return refinedResults

    def pruneExtractedContextualInformation(
            self, drivedValues,
            originalValues
            ):
        #########################################
        # Turn it off:
        # return drivedValues
        ########################################
        lstAcceptedValues = []
        if len(drivedValues) and len(originalValues):
            for subDrivedField in drivedValues:
                for originalField in originalValues:
                    simScore = get_jaccard_sim(
                        subDrivedField.lower(), originalField.lower())
                    if (
                            simScore > self.CommonSubsetThreshold
                            and subDrivedField not in lstAcceptedValues):
                        lstAcceptedValues.append(subDrivedField)
        else:
            lstAcceptedValues = drivedValues

        return lstAcceptedValues

    def post_process_index_record(self, record):
        # Extract contextual information
        extracted_contextual_information = []
        for k in ['description', 'abstract', 'keywords']:
            text = (str(record[k]).replace("[", "")
                    .replace("]", "")
                    .replace("'", "")
                    .replace("\"", "")
                    .replace("\"\"", "")
                    .replace("None", ""))
            if text:
                extracted_contextual_information.append(text)
        # Prune contextual information
        for k in ['potentialTopics', 'EssentialVariables']:
            record[k] = self.pruneExtractedContextualInformation(
                record[k], extracted_contextual_information)
        for k, v in record.items():
            if not isinstance(v, list):
                record[k] = [v]
        return record

    def save_index_record(self, record):
        index_record_filename = os.path.join(
            self.paths.index_records_dir,
            utils.gen_id_from_url(record['url']) + '.json',
            )
        with open(index_record_filename, 'w') as f:
            json.dump(record, f)

    def gen_record_from_url(self, datasetURL):
        pass


class LanguageTools:
    def __init__(self, paths):
        self.paths = paths
        nltk.data.path.append(self.paths.nltk_data_dir)

        self._download_nltk_data()

        self.EnglishTerm = enchant.Dict("en_US")
        self.stop = set(nltk.corpus.stopwords.words('english'))
        self.exclude = set(string.punctuation)
        self.lemma = nltk.WordNetLemmatizer()
        self.spacy_nlp = spacy.load('en_core_web_md')

    def _download_nltk_data(self):
        requirements = [
            ('omw-1.4', 'corpora/omw-1.4.zip'),
            ('wordnet', 'corpora/wordnet.zip'),
            ('stopwords', 'corpora/stopwords.zip'),
            ('punkt', 'tokenizers/punkt'),
            ('averaged_perceptron_tagger',
             'taggers/averaged_perceptron_tagger')
            ]
        for _id, _name in requirements:
            try:
                nltk.data.find(_name)
            except LookupError:
                nltk.download(_id, download_dir=self.paths.nltk_data_dir)

    @staticmethod
    def get_synonyms(word):
        word = word.lower()
        syn_sets = nltk.corpus.wordnet.synsets(word)
        if len(syn_sets) == 0:
            return []
        synonyms = syn_sets[0].lemma_names()
        synonyms = set(s.lower().replace('_', ' ') for s in synonyms)
        if word in synonyms:
            synonyms.remove(word)
        return list(synonyms)

    def get_words_synonyms(self, words):
        synonyms = []
        for word in words:
            synonyms += self.get_synonyms(word)
        return synonyms


def get_html_tags(url, tag):
    with requests.get(url) as f:
        soup = BeautifulSoup(f.content, 'lxml')
    return soup.find_all(tag)


def nested_dict_value(d):
    if type(d) == dict:
        for v in d.values():
            if isinstance(v, dict):
                yield from nested_dict_value(v)
            else:
                yield v


def is_nested_list(lst):
    try:
        next(x for x in lst if isinstance(x, list))
    except StopIteration:
        return False
    return True


def flatten_list(lst):
    if is_nested_list(lst):
        return [str(item) for sublist in lst for item in sublist if
                not type(sublist) == str]
    return lst


def merge_list(contextualText):
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


class DeepSearch:
    def __init__(self):
        self.found_results = []

    def search(self, needles, haystack):
        found = {}
        if not isinstance(needles, list):
            needles = [needles]

        if isinstance(haystack, dict):
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
                                if v not in self.found_results:
                                    self.found_results.append(v)
        elif isinstance(haystack, list):
            for node in haystack:
                result = self.search(needles, node)
                if result:
                    for k, v in result.items():
                        found[k] = v
                        if v not in self.found_results:
                            self.found_results.append(v)
        return found
