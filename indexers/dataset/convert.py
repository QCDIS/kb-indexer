from glob import glob
from tqdm import tqdm
import abc
import json
import os
import re
import string

import enchant
from fuzzywuzzy import fuzz
import gensim
import nltk
import spacy

from .common import Paths


class Converter(abc.ABC):
    contextual_text_fields: list[str]
    contextual_text_fallback_field: str
    RI: str

    similarity_threshold = 0.75
    jaccard_threshold = 0.0

    def __init__(self, paths: Paths):
        self.paths = paths
        self.lt = LanguageTools(paths)

        self.domain = self.get_domain(self.RI)  # TODO convert to list

    def list_metadata(self):
        pattern = os.path.join(self.paths.meta_dir, '*.json')
        return sorted(glob(pattern))

    def convert_all(self):
        for meta_file in tqdm(self.list_metadata(), desc='converting records'):
            with open(meta_file, 'r') as f:
                meta = json.load(f)
            self.convert_record(
                meta['filename'],
                self.paths.converted_file(meta['id']),
                meta,
                )

    @abc.abstractmethod
    def convert_record(self, raw_filename, converted_filename, metadata):
        pass

    def get_contextual_text(self, doc):
        contextual_text = DeepSearch().search(self.contextual_text_fields, doc)
        if not contextual_text:
            contextual_text = DeepSearch().search(
                [self.contextual_text_fallback_field], doc)
        contextual_text = list(nested_dict_value(contextual_text))
        return merge_list(contextual_text)

    @staticmethod
    def extract_textual_content(y):
        out = {}
        values = []

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
                        values.append(text)

        flatten(y)
        return values

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

    def topic_mining(self, doc):
        if not doc:
            return []

        contextual_text = self.get_contextual_text(doc)
        if not contextual_text:
            contextual_text = self.extract_textual_content(doc)

        doc_clean = [self.clean(doc).split() for doc in contextual_text]
        dictionary = gensim.corpora.Dictionary(doc_clean)
        doc_term_matrix = [dictionary.doc2bow(doc) for doc in doc_clean]
        if not doc_term_matrix:
            return []

        lda_model = gensim.models.LdaMulticore(
            corpus=doc_term_matrix,
            id2word=dictionary,
            num_topics=3,
            passes=10,
            )
        topics = lda_model.show_topics(log=True, formatted=True)
        top_topics = sum([re.findall('"([^"]*)"', t[1]) for t in topics], [])
        return list(set(top_topics))

    def get_topics_by_domain_vocabularies(self, topics, domain):
        vocabularies = set()
        with open(self.paths.domainVocbularies_filename) as f:
            domain_vocabularies = json.load(f)
        for vocab in domain_vocabularies[domain]:
            for topic in topics:
                w1 = self.lt.spacy_nlp(topic.lower())
                w2 = self.lt.spacy_nlp(vocab.lower())
                similarity = w1.similarity(w2)
                if similarity > self.similarity_threshold:
                    vocabularies.add(vocab)
        return list(vocabularies)

    def get_essential_variables(self, essential_variables, topics):
        similar_essential_variables = set()
        topics = [*self.lt.get_words_synonyms(topics), *topics]
        for variable in essential_variables:
            for topic in topics:
                w1 = self.lt.spacy_nlp(topic.lower())
                w2 = self.lt.spacy_nlp(variable.lower())
                similarity = w1.similarity(w2)
                if similarity >= self.similarity_threshold:
                    similar_essential_variables.add(variable)
        return list(similar_essential_variables)

    def get_domain_essential_variables(self):
        with open(self.paths.essentialVariabels_filename) as f:
            essential_variables_list = json.load(f)
        return essential_variables_list.get(self.domain)

    def get_RI(self, doc):
        with open(self.paths.RI_filename, "r") as f:
            RI_list = json.load(f)
        textual_contents = self.extract_textual_content(doc)
        for RI_name, RI_tokens in RI_list.items():
            for text in textual_contents:
                for RI_token in RI_tokens:
                    if RI_token in text:
                        return RI_name

    def get_domain(self, RI):
        with open(self.paths.domain_filename, "r") as f:
            domain_list = json.load(f)
        return domain_list.get(RI)[0]

    def refine_results(self, text_array, data_type, property_name):
        data_type = data_type.lower()
        refined_results = set()
        if text_array:
            if type(text_array) == str:
                text_array = [text_array]

            if type(text_array) == dict:
                text_array = list(nested_dict_value(text_array))

            if type(text_array) == list:
                text_array = flatten_list(text_array)
                values = []
                for text in text_array:
                    if type(text) == dict:
                        text = list(nested_dict_value(text))
                        values.append(text)
                    elif type(text) == list:
                        values = values + text
                    else:
                        values.append(text)
                if isinstance(values, list) and len(values):
                    text_array = flatten_list(values)

            for text in text_array:
                doc = self.lt.spacy_nlp(str(text).strip())

                if "url" in data_type and type(text) == str:
                    urls = re.findall(r"(?P<url>https?://\S+)", text)
                    for url in urls:
                        refined_results.add(url)

                if "person" in data_type:
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and ent.label_ == "PERSON":
                                refined_results.add(ent.text)

                if "organization" in data_type:
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and ent.label_ == "ORG":
                                refined_results.add(ent.text)

                if "place" in data_type:
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and (ent.label_ == "GPE" or
                                             ent.label_ == "LOC"):
                                refined_results.add(ent.text)

                if "date" in data_type:
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and ent.label_ == "DATE":
                                refined_results.add(ent.text)

                if "product" in data_type:
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and ent.label_ == "PRODUCT":
                                refined_results.add(ent.text)

                if ("integer" in data_type) or ("number" in data_type):
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and ent.label_ == "CARDINAL":
                                refined_results.add(ent.text)

                if "money" in data_type:
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and ent.label_ == "MONEY":
                                refined_results.add(ent.text)

                if "workofart" in data_type:
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and ent.label_ == "WORK_OF_ART":
                                refined_results.add(ent.text)

                if "language" in data_type:
                    if doc.ents:
                        for ent in doc.ents:
                            if ent.text and (ent.label_ == "LANGUAGE" or
                                             ent.label_ == "GPE"):
                                refined_results.add(ent.text)

                if property_name.lower() not in str(text).lower() and (
                        "text" in data_type or "definedterm" in data_type):
                    refined_results.add(text)

        return list(refined_results)

    def prune_contextual_information(self, values, contextual_values):
        if not values or not contextual_values:
            return values

        retained_values = set()
        for val in values:
            for contextual_val in contextual_values:
                sim_score = get_jaccard_sim(
                    val.lower(),
                    contextual_val.lower(),
                    )
                if sim_score > self.jaccard_threshold:
                    retained_values.add(val)

        return list(retained_values)

    def language_extraction(self, raw_doc, doc):
        doc['potentialTopics'] = self.topic_mining(raw_doc)
        doc['EssentialVariables'] = self.get_essential_variables(
            self.get_domain_essential_variables(),
            doc['potentialTopics'])

    def post_process_doc(self, doc):
        for k, v in doc.items():
            if not isinstance(v, list):
                doc[k] = [v]

        contextual_information = [
            doc['description'],
            doc['keywords'],
            doc['abstract'],
            ]
        contextual_information = flatten_list(contextual_information)

        for k in ['potentialTopics', 'EssentialVariables']:
            doc[k] = self.prune_contextual_information(
                doc[k], contextual_information)

        with open(self.paths.metadataStar_filename, "r") as f:
            schema = json.loads(f.read())
        for k, v in doc.items():
            doc[k] = self.refine_results(v, schema[k][0], k)

    @staticmethod
    def save_index_record(record, filename):
        with open(filename, 'w') as f:
            json.dump(record, f, indent=2, sort_keys=True)


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
