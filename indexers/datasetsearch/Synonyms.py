import nltk
from nltk.corpus import wordnet

def download_nltk_dependencies_if_needed():
    nltk.data.path.append("var/www/nltk_data")
    try:
        nltk.word_tokenize('foobar')
    except LookupError:
        nltk.download('punkt', download_dir='var/www/nltk_data')
    try:
        nltk.pos_tag(nltk.word_tokenize('foobar'))
    except LookupError:
        nltk.download('averaged_perceptron_tagger', download_dir='var/www/nltk_data')

def get_some_word_synonyms(word):
    word = word.lower()
    synonyms = []
    synsets = wordnet.synsets(word)
    if (len(synsets) == 0):
        return []
    synset = synsets[0]
    lemma_names = synset.lemma_names()
    for lemma_name in lemma_names:
        lemma_name = lemma_name.lower().replace('_', ' ')
        if (lemma_name != word and lemma_name not in synonyms):
            synonyms.append(lemma_name)
    return synonyms

def get_all_word_synonyms(word):
    word = word.lower()
    synonyms = []
    synsets = wordnet.synsets(word)
    if (len(synsets) == 0):
        return []
    for synset in synsets:
        lemma_names = synset.lemma_names()
        for lemma_name in lemma_names:
            lemma_name = lemma_name.lower().replace('_', ' ')
            if (lemma_name != word and lemma_name not in synonyms):
                synonyms.append(lemma_name)
    return synonyms

download_nltk_dependencies_if_needed()


def getSynonyms(words):
    lstSynonyms=[]
    for word in words:
        synonyms=get_some_word_synonyms(word)
        for synonym in synonyms:
            lstSynonyms.append(synonym)
    return lstSynonyms
