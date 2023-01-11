from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Index
import json
import shlex
import subprocess
import requests
import os
from urllib.parse import urlparse
from datasetsearch.WebCrawler import Crawler
#from LanguageDetection import LangaugePrediction
from datasetsearch.Synonyms import getSynonyms
from os import walk
from bs4 import BeautifulSoup
import requests
import json
import sys
import re
import spacy
import nltk
import numpy as np
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
import string
import gensim
from gensim import corpora
import enchant
from fuzzywuzzy import fuzz
from SPARQLWrapper import SPARQLWrapper, JSON
import urllib.request
import uuid
import json, xmljson
from lxml.etree import fromstring, tostring
import xml.etree.ElementTree as ET
import glob
import csv
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
#----------------------------------------------------------------------------------------
#nltk.data.path.append("/home/siamak/nltk_data")
#nltk.data.path.append("var/www/nltk_data")

nltk_data_dir = os.path.join(os.path.dirname(__file__), 'nltk_data')
nltk.data.path.append(nltk_data_dir)
nltk.download('wordnet', download_dir=nltk_data_dir)
nltk.download('stopwords', download_dir=nltk_data_dir)

#----------------------------------------------------------------------------------------
EnglishTerm = enchant.Dict("en_US")
stop = set(stopwords.words('english'))
exclude = set(string.punctuation)
lemma = WordNetLemmatizer()
Lda = gensim.models.ldamodel.LdaModel
spacy_nlp  = spacy.load('en_core_web_md')
#----------------------------------------------------------------------------------------
cwd= os.path.dirname(__file__)
currentRun="Run 15/"
MetaDataRecordPath=cwd+"/Metadata records/"
ICOS__MetadataRecordsFileName="ICOS-metadata-records.json"
SeaDataNet_CDI__MetadataRecordsFileName= "SeaDataNet-CDI-metadata-records.xml"
SeaDataNet_EDMED__MetadataRecordsFileName= "SeaDataNet-EDMED-metadata-records.json"
Lifewatch__MetadataRecordsFileName= "LifeWatch.txt"
metadataStar_root=cwd+"/Metadata*/metadata*.json"
RI_root=cwd+"/Metadata*/RIs.json"
indexFiles_root=cwd+"/index files/"+currentRun
domain_root=cwd+"/Metadata*/domain.json"
essentialVariabels_root=cwd+"/Metadata*/essential_variables.json"
domainVocbularies_root=cwd+"/Metadata*/Vocabularies.json"
os.makedirs(indexFiles_root, exist_ok=True)
os.makedirs(MetaDataRecordPath, exist_ok=True)
os.makedirs(cwd+"/Metadata*", exist_ok=True)
#----------------------------------------------------------------------------------------
acceptedSimilarityThreshold=0.75
CommonSubsetThreshold=0.0
contextualInfo=""
#----------------------------------------------------------------------------------------
#-------------------Lifewatch
def getDatasetRecords__LifeWatch():
    driver = webdriver.Chrome(ChromeDriverManager().install())
    driver.get("https://metadatacatalogue.lifewatch.eu/srv/eng/catalog.search#/search?facet.q=type%2Fdataset&resultType=details&sortBy=relevance&from=301&to=400&fast=index&_content_type=json")
#    elem = driver.find_element_by_name("q")
    print (print(driver.title))
    print("Lifewatch data collection is done!")
    driver.close()
#-------------------SeaDataNet
def getDatasetRecords__SeaDataNet_EDMED():
    with urllib.request.urlopen('https://edmed.seadatanet.org/sparql/sparql?query=select+%3FEDMEDRecord+%3FTitle+where+%7B%3FEDMEDRecord+a+%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fdcat%23Dataset%3E+%3B+%3Chttp%3A%2F%2Fpurl.org%2Fdc%2Fterms%2Ftitle%3E+%3FTitle+.%7D+&output=json&stylesheet=') as f:
        data = f.read().decode('utf-8')
    json_data = json.loads(r''+data)
    indexFile= open(MetaDataRecordPath+SeaDataNet_EDMED__MetadataRecordsFileName,"w+")
    indexFile.write(json.dumps(json_data))
    indexFile.close()
    print("SeaDataNet EDMED data collection is done!")
#-------------------
def getDatasetRecords__SeaDataNet_CDI():
    with urllib.request.urlopen('https://cdi.seadatanet.org/report/aggregation') as f:
        data = f.read().decode('utf-8')
    indexFile= open(MetaDataRecordPath+SeaDataNet_CDI__MetadataRecordsFileName,"w+")
    indexFile.write(data)
    indexFile.close()
    print("SeaDataNet CDI data collection is done!")
#-------------------
def getDatasetRecords__ICOS():
    cURL = r"""curl https://meta.icos-cp.eu/sparql -H 'Cache-Control: no-cache' -X POST --data 'query=prefix%20cpmeta%3A%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fontologies%2Fcpmeta%2F%3E%0Aprefix%20prov%3A%20%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fprov%23%3E%0Aselect%20%3Fdobj%20%3Fspec%20%3FfileName%20%3Fsize%20%3FsubmTime%20%3FtimeStart%20%3FtimeEnd%0Awhere%20%7B%0A%09VALUES%20%3Fspec%20%7B%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FradonFluxSpatialL3%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2Fco2EmissionInventory%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FsunInducedFluorescence%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FoceanPco2CarbonFluxMaps%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FinversionModelingSpatial%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FbiosphereModelingSpatial%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FecoFluxesDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FecoEcoDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FecoMeteoDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FecoAirTempMultiLevelsDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FecoProfileMultiLevelsDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcMeteoL0DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcLosGatosL0DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcPicarroL0DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FingosInversionResult%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2Fsocat_DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcBioMeteoRawSeriesBin%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcStorageFluxRawSeriesBin%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcBioMeteoRawSeriesCsv%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcStorageFluxRawSeriesCsv%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcSaheatFlagFile%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FceptometerMeasurements%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FglobalCarbonBudget%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FnationalCarbonEmissions%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FglobalMethaneBudget%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FdigHemispherPics%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcEddyFluxRawSeriesCsv%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcEddyFluxRawSeriesBin%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCh4L2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCoL2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCo2L2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcMtoL2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcC14L2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcMeteoGrowingNrtDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCo2NrtGrowingDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCh4NrtGrowingDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcN2oL2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCoNrtGrowingDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcN2oNrtGrowingDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FingosCh4Release%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FingosN2oRelease%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcRnNrtDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2Fdrought2018AtmoProduct%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FmodelDataArchive%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcArchiveProduct%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2Fdought2018ArchiveProduct%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatmoMeasResultsArchive%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcNrtAuxData%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcFluxnetProduct%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2Fdrought2018FluxnetProduct%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcNrtFluxes%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcNrtMeteosens%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcNrtMeteo%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FicosOtcL1Product%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FicosOtcL1Product_v2%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FicosOtcL2Product%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FicosOtcFosL2Product%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FotcL0DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FinversionModelingTimeseries%3E%7D%0A%09%3Fdobj%20cpmeta%3AhasObjectSpec%20%3Fspec%20.%0A%09%3Fdobj%20cpmeta%3AhasSizeInBytes%20%3Fsize%20.%0A%3Fdobj%20cpmeta%3AhasName%20%3FfileName%20.%0A%3Fdobj%20cpmeta%3AwasSubmittedBy%2Fprov%3AendedAtTime%20%3FsubmTime%20.%0A%3Fdobj%20cpmeta%3AhasStartTime%20%7C%20%28cpmeta%3AwasAcquiredBy%20%2F%20prov%3AstartedAtTime%29%20%3FtimeStart%20.%0A%3Fdobj%20cpmeta%3AhasEndTime%20%7C%20%28cpmeta%3AwasAcquiredBy%20%2F%20prov%3AendedAtTime%29%20%3FtimeEnd%20.%0A%09FILTER%20NOT%20EXISTS%20%7B%5B%5D%20cpmeta%3AisNextVersionOf%20%3Fdobj%7D%0A%7D%0Aorder%20by%20desc%28%3FsubmTime%29'"""
    lCmd = shlex.split(cURL) # Splits cURL into an array
    p = subprocess.Popen(lCmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate() # Get the output and the err message
    json_data = json.loads(r''+out.decode("utf-8"))
    indexFile= open(MetaDataRecordPath+ICOS__MetadataRecordsFileName,"w+")
    indexFile.write(json.dumps(json_data))
    indexFile.close()
    print("ICOS data collection is done!")
#----------------------------------------------------------------------------------------
def getOnlineDatasetRecords__LifeWatch(rnd,genRnd,startingPoint):
    indexFile = os.path.join(MetaDataRecordPath+Lifewatch__MetadataRecordsFileName)
    cnt=1
    c=0
    lstDatasetCollection=[]
    with open(indexFile) as f:
        lines = f.readlines()
    random_selection= random.sample(range(startingPoint, len(lines)), genRnd)
    baseline= "https://metadatacatalogue.lifewatch.eu/srv/api/records/"
    xml="/formatters/xml?approved=true"
    for line in lines:
        if cnt in random_selection or not(rnd):
            datasetID=(line.split(",")[1].replace("\"",""))
            if not "oai:marineinfo.org:id:dataset" in datasetID:
                continue
            c=c+1
            url=baseline
            lstDatasetCollection.append(url+datasetID+xml)
            cnt=cnt+1
    saveSelectedURLs(lstDatasetCollection, "LifeWatch")
    return lstDatasetCollection
#----------------------------------------------------------------------------------------
def getOnlineDatasetRecords__ICOS(rnd,genRnd,startingPoint):
    indexFile= open(MetaDataRecordPath+ICOS__MetadataRecordsFileName,"r")
    dataset_json = json.loads(r''+indexFile.read())

    cnt=1
    random_selection= random.sample(range(startingPoint, len(dataset_json["results"]["bindings"])), genRnd)
    c=0

    lstDatasetCollection=[]

    for record in dataset_json["results"]["bindings"]:
        if cnt in random_selection or not(rnd):
            c=c+1
            filename=record["fileName"]["value"]
            landingPage=record["dobj"]["value"]
            timeEnd=record["timeEnd"]["value"]
            timeStart=record["timeStart"]["value"]
            submTime=record["submTime"]["value"]
            size =record["size"]["value"]
            spec=record["spec"]["value"]
            #downloadableLink= "https://data.icos-cp.eu/objects/"+os.path.basename(urlparse(landingPage).path)

            lstDatasetCollection.append(landingPage)
        cnt=cnt+1
    saveSelectedURLs(lstDatasetCollection, "ICOS")
    return lstDatasetCollection

#----------------------------------------------------------------------------------------
#{"EDMEDRecord": {"type": "uri", "value": "https://edmed.seadatanet.org/report/6325/"},
# "Title": {"type": "literal", "xml:lang": "en", "value": "Water Framework Directive (WFD) Greece (2012-2015)"}}

import random

def getOnlineDatasetRecords__SeaDataNet_EDMED(rnd,genRnd,startingPoint):
    indexFile= open(MetaDataRecordPath+SeaDataNet_EDMED__MetadataRecordsFileName,"r")
    dataset_json = json.loads(r''+indexFile.read())
    cnt=1
    random_selection= random.sample(range(startingPoint, len(dataset_json["results"]["bindings"])), genRnd)
    c=0
    lstDatasetCollection=[]

    for record in dataset_json["results"]["bindings"]:
        if cnt in random_selection or not(rnd):
            c=c+1
            landingPage=record["EDMEDRecord"]["value"]
            title=record["Title"]["value"]
            lstDatasetCollection.append(landingPage)
        cnt=cnt+1
    saveSelectedURLs(lstDatasetCollection, "SeaDataNet_EDMED")
    return lstDatasetCollection
#----------------------------------------------------------------------------------------
def getOnlineDatasetRecords__SeaDataNet_CDI(rnd,genRnd,startingPoint):
    tree = ET.parse(MetaDataRecordPath+SeaDataNet_CDI__MetadataRecordsFileName)
    indexFile = tree.getroot()
    cnt=1
    random_selection= random.sample(range(startingPoint, len(indexFile)), genRnd)
    c=0
    lstDatasetCollection=[]
    for record in indexFile:
        if cnt in random_selection or not(rnd):
            c=c+1
            url=record.text
            pos=url.rfind("/xml")
            if(pos and pos+4==len(url)):
                url=url.replace("/xml","/json")
            lstDatasetCollection.append(url)
        cnt=cnt+1
    saveSelectedURLs(lstDatasetCollection, "SeaDataNet_CDI")
    return lstDatasetCollection

#----------------------------------------------------------------------------------------
def getRI(dataset_JSON):
    RI_content = open(RI_root,"r")
    RI_json = json.loads(r''+RI_content.read())
    dataset_content=extractTextualContent(dataset_JSON)
    for RI in RI_json:
        for RI_keys in RI_json[RI]:
            for ds in dataset_content:
                if RI_keys in ds:
                    return  RI
#----------------------------------------------------------------------------------------
def getDomain(RI_seed):
    domain_content = open(domain_root,"r")
    domain_json = json.loads(r''+domain_content.read())
    for RI in domain_json:
        if RI == RI_seed:
            return domain_json[RI]

#----------------------------------------------------------------------------------------
def extractTextualContent(y):
    out = {}
    lstvalues=[]
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
            text=x
            if type(text)==list or type(text)==dict:
                text=" ".join(str(x) for x in text)
            if type(text)==str and len(text)>1:
                text=re.sub(r'http\S+', '', text)
                if type(text)==str and len(text)>1:
                    lstvalues.append(text)
    flatten(y)
    return lstvalues
#----------------------------------------------------------------------------------------
def clean(doc):
    integer_free = ''.join([i for i in doc if not i.isdigit()])
    stop_free = " ".join([i for i in integer_free.lower().split() if i not in stop])
    punc_free = ''.join(ch for ch in stop_free if ch not in exclude)
    normalized = " ".join(lemma.lemmatize(word) for word in punc_free.split() if len(word)>2 and EnglishTerm.check(word))
    return normalized
#----------------------------------------------------------------------------------------
def topicMining(dataset_json,RI):
    #########################################
    # Turn it off:
#    if RI=="SeaDataNet_EDMED": Jsontext=getContextualText_SeaDataNet_EDMED(dataset_json)
#    if RI=="SeaDataNet_CDI": Jsontext= getContextualText_SeaDataNet_CDI (dataset_json)
#    if RI=="LifeWatch": Jsontext= getContextualText_LifeWatch (dataset_json)
#    if RI=="ICOS": Jsontext= getContextualText_ICOS (dataset_json)
#    return Jsontext
    ########################################
    lsttopic=[]
    Jsontext=""
    if(dataset_json!=""):
        Jsontext=""
        if RI=="SeaDataNet_EDMED": Jsontext=getContextualText_SeaDataNet_EDMED(dataset_json)
        if RI=="SeaDataNet_CDI": Jsontext= getContextualText_SeaDataNet_CDI (dataset_json)
        if RI=="LifeWatch": Jsontext= getContextualText_LifeWatch (dataset_json)
        if RI=="ICOS": Jsontext= getContextualText_ICOS (dataset_json)
        if not len(Jsontext):
            Jsontext=extractTextualContent(dataset_json)
        doc_clean = [clean(doc).split() for doc in Jsontext]
        dictionary = corpora.Dictionary(doc_clean)
        doc_term_matrix = [dictionary.doc2bow(doc) for doc in doc_clean]
        if len(doc_term_matrix)>0:
            ldamodel = gensim.models.LdaMulticore(corpus=doc_term_matrix,id2word=dictionary,num_topics=3,passes=10)
            topics=ldamodel.show_topics(log=True, formatted=True)
            topTopics= sum([re.findall('"([^"]*)"',listToString(t[1])) for t in topics],[])
            for topic in topTopics:
                lsttopic.append(topic) if topic not in lsttopic else lsttopic
    return lsttopic
#----------------------------------------------------------------------------------------
def listToString(s):
    str1 = ""
    for ele in s:
        str1 += ele
    return str1
#----------------------------------------------------------------------------------------
def getSimilarEssentialVariables(essentialVariables, topics):
    lstEssentialVariables=[]
    lsttopics= [*getSynonyms(topics), *topics]
    for variable in essentialVariables:
        for topic in lsttopics:
            w1=spacy_nlp(topic.lower())
            w2=spacy_nlp(variable.lower())
            similarity=w1.similarity(w2)
            if similarity >= acceptedSimilarityThreshold:
                lstEssentialVariables.append(variable) if variable not in lstEssentialVariables else lstEssentialVariables
    return lstEssentialVariables
#----------------------------------------------------------------------------------------
def getDomainEssentialVariables(domain):
    essentialVariabels_content = open(essentialVariabels_root,"r")
    essentialVariabels_json = json.loads(r''+essentialVariabels_content.read())
    for domainVar in essentialVariabels_json:
        if domain==domainVar:
            return essentialVariabels_json[domain]
#----------------------------------------------------------------------------------------
def flatten_dict(dd, separator='_', prefix=''):
    return { prefix + separator + k if prefix else k : v
             for kk, vv in dd.items()
             for k, v in flatten_dict(vv, separator, kk).items()
             } if isinstance(dd, dict) else { prefix : dd }
#----------------------------------------------------------------------------------------
def NestedDictValues(d):
    for v in d.values():
        if isinstance(v, dict):
            yield from NestedDictValues(v)
        else:
            yield v
#----------------------------------------------------------------------------------------
def remove_none(obj):
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(remove_none(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return type(obj)((remove_none(k), remove_none(v)) for k, v in obj.items() if k is not None and v is not None)
    else:
        return obj
#----------------------------------------------------------------------------------------
def is_nested_list(l):
    try:
        next(x for x in l if isinstance(x,list))
    except StopIteration:
        return False
    return True
#----------------------------------------------------------------------------------------
def flatten_list(t):
    if(is_nested_list(t)):
        return [str(item) for sublist in t for item in sublist if not type(sublist)==str]
    return t
#----------------------------------------------------------------------------------------
#########################################
# Turn it off: refineResults_ -> refineResults

#def refineResults(TextArray,datatype,proprtyName):
#    datatype=datatype.lower()
#    refinedResults=[]
#    if len(TextArray):
#        if type(TextArray)==str:
#            TextArray=[TextArray]
#        if type(TextArray)==dict:
#           TextArray=list(NestedDictValues(TextArray))

#        if type(TextArray)==list:
#            TextArray=flatten_list(TextArray)
#            values=[]
#            for text in TextArray:
#                if type(text)==dict:
#                    text= list(NestedDictValues(text))
#                    values.append(text)
#                elif type(text)==list:
#                    values=values+text
#                else:
#                    values.append(text)
#            if type (values) == list and len(values):
#                TextArray=flatten_list(values)
#        if type(TextArray)==type(None):
#            TextArray=["\""+str(TextArray)+"\""]

#        for text in TextArray:
            #..................................................................................
#            if proprtyName.lower() not in str(text).lower() and ("text" in datatype or "definedterm" in datatype):
#                refinedResults.append(text) if text not in refinedResults else refinedResults
            #..................................................................................
#    return refinedResults
#########################################
def refineResults(TextArray,datatype,proprtyName):
    datatype=datatype.lower()
    refinedResults=[]
    if len(TextArray):
        if type(TextArray)==str:
            TextArray=[TextArray]

        if type(TextArray)==dict:
            TextArray=list(NestedDictValues(TextArray))

        if type(TextArray)==list:
            TextArray=flatten_list(TextArray)
            values=[]
            for text in TextArray:
                if type(text)==dict:
                    text= list(NestedDictValues(text))
                    values.append(text)
                elif type(text)==list:
                    values=values+text
                else:
                    values.append(text)
            if type (values) == list and len(values):
                TextArray=flatten_list(values)
        if type(TextArray)==type(None):
            TextArray=["\""+str(TextArray)+"\""]

        for text in TextArray:
            doc = spacy_nlp(str(text).strip())
            #..................................................................................
            if ("url" in datatype and type(text)==str):
                urls = re.findall("(?P<url>https?://[^\s]+)", text)
                if len(urls):
                    refinedResults.append(urls) if urls not in refinedResults else refinedResults
            #..................................................................................
            if ("person" in datatype):
                if doc.ents:
                    for ent in doc.ents:
                        if (len(ent.text)>0) and ent.label_=="PERSON":
                            refinedResults.append(ent.text) if ent.text not in refinedResults else refinedResults
            #..................................................................................
            if ("organization" in datatype):
                if doc.ents:
                    for ent in doc.ents:
                        if(len(ent.text)>0) and ent.label_=="ORG":
                            refinedResults.append(ent.text) if ent.text not in refinedResults else refinedResults
            #..................................................................................
            if ("place" in datatype):
                if doc.ents:
                    for ent in doc.ents:
                        if(len(ent.text)>0) and ent.label_=="GPE" or ent.label_=="LOC":
                            refinedResults.append(ent.text) if ent.text not in refinedResults else refinedResults
            #..................................................................................
            if ("date" in datatype):
                if doc.ents:
                    for ent in doc.ents:
                        if(len(ent.text)>0) and ent.label_=="DATE":
                            refinedResults.append(ent.text) if ent.text not in refinedResults else refinedResults
            #..................................................................................
            if ("product" in datatype):
                if doc.ents:
                    for ent in doc.ents:
                        if(len(ent.text)>0) and ent.label_=="PRODUCT":
                            refinedResults.append(ent.text) if ent.text not in refinedResults else refinedResults
            #..................................................................................
            if ("integer" in datatype) or ("number" in datatype ):
                if doc.ents:
                    for ent in doc.ents:
                        if(len(ent.text)>0) and ent.label_=="CARDINAL":
                            refinedResults.append(ent.text) if ent.text not in refinedResults else refinedResults
            #..................................................................................
            if ("money" in datatype):
                if doc.ents:
                    for ent in doc.ents:
                        if(len(ent.text)>0) and ent.label_=="MONEY":
                            refinedResults.append(ent.text) if ent.text not in refinedResults else refinedResults
            #..................................................................................
            if ("workofart" in datatype):
                if doc.ents:
                    for ent in doc.ents:
                        if(len(ent.text)>0) and ent.label_=="WORK_OF_ART":
                            refinedResults.append(ent.text) if ent.text not in refinedResults else refinedResults
            #..................................................................................
            if ("language" in datatype):
                if doc.ents:
                    for ent in doc.ents:
                        if(len(ent.text)>0) and ent.label_=="LANGUAGE" or ent.label_=="GPE":
                            refinedResults.append(ent.text) if ent.text not in refinedResults else refinedResults
            #..................................................................................
            if proprtyName.lower() not in str(text).lower() and ("text" in datatype or "definedterm" in datatype):
                refinedResults.append(text) if text not in refinedResults else refinedResults
            #..................................................................................
    return refinedResults
#----------------------------------------------------------------------------------------
foundResults=[]
def deep_search(needles, haystack):
    found = {}
    if type(needles) != type([]):
        needles = [needles]

    if type(haystack) == type(dict()):
        for needle in needles:
            if needle in haystack.keys():
                found[needle] = haystack[needle]
            elif len(haystack.keys()) > 0:
                #--------------------- Fuzzy calculation
                for key in haystack.keys():
                    if fuzz.ratio(needle.lower(),key.lower()) > 75:
                        found[needle] = haystack[key]
                        break
                #--------------------- ^
                for key in haystack.keys():
                    result = deep_search(needle, haystack[key])
                    if result:
                        for k, v in result.items():
                            found[k] = v
                            foundResults.append(v) if v not in foundResults else foundResults
    elif type(haystack) == type([]):
        for node in haystack:
            result = deep_search(needles, node)
            if result:
                for k, v in result.items():
                    found[k] = v
                    foundResults.append(v) if v not in foundResults else foundResults
    return found
#----------------------------------------------------------------------------------------
def searchField(field,datatype,json):
    foundResults.clear()
    deep_search(field,json)
    refinedResults=refineResults(foundResults,datatype,field)
    return refinedResults
#----------------------------------------------------------------------------------------
def getTopicsByDomainVocabulareis(topics,domain):
    Vocabs=[]
    domainVocbularies_content = open(domainVocbularies_root,"r")
    domainVocbularies_object = json.loads(r''+domainVocbularies_content.read())
    for vocab in domainVocbularies_object[domain]:
        for topic in topics:
            w1=spacy_nlp(topic.lower())
            w2=spacy_nlp(vocab.lower())
            similarity=w1.similarity(w2)
            if similarity > acceptedSimilarityThreshold:
                Vocabs.append(vocab) if vocab not in Vocabs else Vocabs
    return Vocabs
#----------------------------------------------------------------------------------------
def datasetProcessing_ICOS(datasetURL):
    metadataStar_content = open(metadataStar_root,"r")
    metadataStar_object = json.loads(r''+metadataStar_content.read())
    unique_filename = str(uuid.uuid4())
    indexfname = os.path.join(indexFiles_root,"ICOS_"+unique_filename)
    indexFile= open(indexfname+".json","w+")
    essentialVariables=[]
    logfile = os.path.join(indexFiles_root,"logfile.csv")
    CSVvalue=""
    if not os.path.exists(logfile):
        logfile= open(logfile,"a+")
        for metadata_property in metadataStar_object:
            CSVvalue=CSVvalue+metadata_property+","
        CSVvalue=CSVvalue+"Precision (EV), Recall (EV), Accuracy (EV), F (EV), Precision (To), Recall (To), Accuracy (To), F (To),"
        logfile.write(CSVvalue)
    else:
        logfile= open(logfile,"a+")

    CSVvalue="\n"

    indexFile.write("{\n")

    scripts=Crawler().getHTMLContent(datasetURL,"script")
    for script in scripts:
        if '<script type="application/ld+json">' in str(script):
            script=str(script)
            start = script.find('<script type="application/ld+json">') + len('<script type="application/ld+json">')
            end = script.find("</script>")
            script = script[start:end]
            JSON=json.loads(r''+script)
            RI=""
            domains=""
            topics=[]
            originalValues=[]
            cnt=0
            lstKeywords=[]
            for metadata_property in metadataStar_object:
                cnt=cnt+1

                if metadata_property=="ResearchInfrastructure":
                    result= getRI(JSON)
                elif metadata_property=="theme":
                    if not len(RI):
                        #RI= getRI(JSON)
                        RI="ICOS"
                    if not len(domains):
                        domains = getDomain(RI)
                    if not len(topics):
                        topics=topicMining(JSON,"ICOS")
                    result=getTopicsByDomainVocabulareis(topics,domains[0])
                elif metadata_property=="potentialTopics":
                    if not len(topics):
                        topics=topicMining(JSON,"ICOS")
                    result=topics
                    result= pruneExtractedContextualInformation(result, originalValues)
                elif metadata_property=="EssentialVariables":
                    if not len(RI):
                        RI= getRI(JSON)
                    if not len(domains):
                        domains = getDomain(RI)
                    if not len(topics):
                        topics=topicMining(JSON, "ICOS")
                    essentialVariables=getDomainEssentialVariables(domains[0])
                    result=getSimilarEssentialVariables(essentialVariables,topics)
                    result= pruneExtractedContextualInformation(result, originalValues)
                elif metadata_property=="url":
                    result=datasetURL#[str(datasetURL)]
                else:
                    result=deep_search([metadata_property],JSON)
                    if not len(result):
                        searchFields=[]
                        for i in range (3, len(metadataStar_object[metadata_property])):
                            result=deep_search([metadataStar_object[metadata_property][i]],JSON)
                            if len(result): searchFields.append(result)
                        result=searchFields
                propertyDatatype=metadataStar_object[metadata_property][0]
                #if metadata_property!="url":
                result=refineResults(result,propertyDatatype,metadata_property)

                #if metadata_property=="language" and (result=="" or result==[]):
                 #   result= LangaugePrediction(extractTextualContent(JSON))

                if(cnt==len(metadataStar_object)):
                    extrachar="\n"
                else:
                    extrachar=",\n"

                flattenValue=str(flatten_list(result))
                if flattenValue=="[None]":
                    flattenValue="[]"

                if (metadata_property=="description" or metadata_property=="keywords" or metadata_property=="abstract"):
                    txtVal=flattenValue.replace("[","").replace("]","").replace("'","").replace("\"","").replace("\"\"","").replace("None","")
                    if txtVal!="":
                        originalValues.append(txtVal)

                if (metadata_property=="landingPage"):
                    flattenValue="[]"


                indexFile.write("\""+str(metadata_property)+"\" :"+
                                flattenValue.replace("']","\"]").replace("['","[\"").replace("',","\",").replace(", '",", \"").replace("\"\"","\"")
                                +extrachar)
                CSVvalue=CSVvalue+flattenValue.replace(",","-").replace("[","").replace("]","").replace("'","").replace("\"","")+","

                if metadata_property=="keywords":
                    lstKeywords=flattenValue

    indexFile.write("}")
    indexFile.close()
    Precision, Recall, Accuracy, F=  metadataRecord_similarity_evaluation(indexfname+".json",'EssentialVariables',['description','keywords','abstract'],essentialVariables)
    CSVvalue=CSVvalue+ str(Precision)+","+ str(Recall)+"," +str(Accuracy)+","+ str(F) +","
    Precision, Recall, Accuracy, F = metadataRecord_similarity_evaluation(indexfname+".json",'potentialTopics',['description','keywords','abstract'],lstKeywords.split(","))
    CSVvalue=CSVvalue+ str(Precision)+","+ str(Recall)+"," +str(Accuracy)+","+ str(F) +","
    logfile.write(CSVvalue)
    logfile.close()

#----------------------------------------------------------------------------------------
def cleanhtml(raw_html):
    CLEANR = re.compile('<.*?>')
    cleantext = re.sub(CLEANR, '', raw_html)
    return  (''.join(x for x in cleantext if x in string.printable)).replace("'","").replace("\"","").strip()
#----------------------------------------------------------------------------------------
lstCoveredFeaturesSeaDataNet=[]
def getValueHTML_SeaDataNet(searchTerm, datasetContents):
    for datasetContent in datasetContents:
        datasetContent=str(datasetContent)
        if searchTerm in datasetContent and searchTerm not in lstCoveredFeaturesSeaDataNet:
            lstCoveredFeaturesSeaDataNet.append(searchTerm)
            return cleanhtml(datasetContent)[len(searchTerm):]
#----------------------------------------------------------------------------------------
def datasetProcessing_SeaDataNet_EDMED(datasetURL):
    metadataStar_content = open(metadataStar_root,"r")
    metadataStar_object = json.loads(r''+metadataStar_content.read())
    unique_filename = str(uuid.uuid4())
    indexfname = os.path.join(indexFiles_root,"SeaDataNet_EDMED_"+unique_filename)
    indexFile= open(indexfname+".json","w+")
    logfile = os.path.join(indexFiles_root,"logfile.csv")
    CSVvalue=""
    originalValues=[]
    if not os.path.exists(logfile):
        logfile= open(logfile,"a+")
        for metadata_property in metadataStar_object:
            CSVvalue=CSVvalue+metadata_property+","
        CSVvalue=CSVvalue+"Precision (EV), Recall (EV), Accuracy (EV), F (EV), Precision (To), Recall (To), Accuracy (To), F (To),"
        logfile.write(CSVvalue)
    else:
        logfile= open(logfile,"a+")
    EDMED_JSON={}
    indexFile.write("{\n")
    datasetContents=Crawler().getHTMLContent(datasetURL,"tr")
    lstCoveredFeaturesSeaDataNet.clear()
    mapping={}
    cnt=0
    lstKeywords=[]
    mapping["url"]=str(datasetURL)
    mapping["ResearchInfrastructure"]="SeaDataNet"
    RI="SeaDataNet"

    value=(getValueHTML_SeaDataNet("Data set name", datasetContents))
    mapping["name"]=str(value)
    EDMED_JSON["name"]=value

    value=(getValueHTML_SeaDataNet("Data holding centre", datasetContents))
    mapping["copyrightHolder"]=str(value)
    mapping["contributor"]=str(value)
    EDMED_JSON["contributor"]=value

    value=(getValueHTML_SeaDataNet("Country", datasetContents))
    mapping["locationCreated"]=str(value)
    mapping["contentLocation"]=str(value)
    EDMED_JSON["contentLocation"]=value

    value=(getValueHTML_SeaDataNet("Time period", datasetContents))
    mapping["contentReferenceTime"]=str(value)
    mapping["datePublished"]=str(value)
    mapping["dateCreated"]=str(value)
    EDMED_JSON["dateCreated"]=value

    value=(getValueHTML_SeaDataNet("Geographical area", datasetContents))
    mapping["spatialCoverage"]=str(value)
    EDMED_JSON["spatialCoverage"]=value

    value=(getValueHTML_SeaDataNet("Parameters", datasetContents))
    mapping["keywords"]=str(value)
    EDMED_JSON["keywords"]=value
    if type(value)==str or type(value)==list:
        lstKeywords=str(value)

    value=(getValueHTML_SeaDataNet("Instruments", datasetContents))
    mapping["measurementTechnique"]=str(value)
    EDMED_JSON["measurementTechnique"]=value

    value=(getValueHTML_SeaDataNet("Summary", datasetContents))
    mapping["description"]=str(value)
    mapping["abstract"]=str(value)
    EDMED_JSON["abstract"]=value

    #mapping["language"]=[LangaugePrediction(TextualContents)]

    value=(getValueHTML_SeaDataNet("Originators", datasetContents))
    mapping["creator"]=str(value)
    EDMED_JSON["creator"]=value

    value=(getValueHTML_SeaDataNet("Data web site", datasetContents))
    mapping["distributionInfo"]=str(value)
    EDMED_JSON["distributionInfo"]=value

    value=(getValueHTML_SeaDataNet("Organisation", datasetContents))
    mapping["publisher"]=str(value)
    EDMED_JSON["publisher"]=value

    value=(getValueHTML_SeaDataNet("Contact", datasetContents))
    mapping["author"]=str(value)
    EDMED_JSON["author"]=value

    value=(getValueHTML_SeaDataNet("Address", datasetContents))
    mapping["contact"]=str(value)
    EDMED_JSON["contact"]=value

    value=(getValueHTML_SeaDataNet("Collating centre", datasetContents))
    mapping["producer"]=str(value)
    mapping["provider"]=str(value)
    EDMED_JSON["provider"]=value

    value=(getValueHTML_SeaDataNet("Local identifier", datasetContents))
    mapping["identifier"]=str(value)
    EDMED_JSON["identifier"]=value

    value=(getValueHTML_SeaDataNet("Last revised", datasetContents))
    mapping["modificationDate"]=str(value)
    EDMED_JSON["modificationDate"]=value

    domains = getDomain(RI)
    value=topicMining(EDMED_JSON,"SeaDataNet_EDMED")
    mapping["potentialTopics"]=value

    essentialVariables=getDomainEssentialVariables(domains[0])
    value=getSimilarEssentialVariables(essentialVariables,value)
    mapping["EssentialVariables"]=value

    CSVvalue="\n"

    for metadata_property in metadataStar_object:
        cnt=cnt+1
        if(cnt==len(metadataStar_object)):
            extrachar="\n"
        else:
            extrachar=",\n"

        if metadata_property in mapping:
            value=mapping[metadata_property]
            if type( mapping[metadata_property])!=list:
                value=[value]

            if (metadata_property=="description" or metadata_property=="keywords" or metadata_property=="abstract"):
                txtVal=str(value).replace("[","").replace("]","").replace("'","").replace("\"","").replace("\"\"","").replace("None","")
                if txtVal!="":
                    originalValues.append(txtVal)

            elif metadata_property=="potentialTopics" or metadata_property=="EssentialVariables":
                value= pruneExtractedContextualInformation(value, originalValues)

            indexFile.write("\""+str(metadata_property)+"\" :"+str(value).replace("'","\"")+extrachar)
            CSVvalue=CSVvalue+str(value).replace(",","-").replace("[","").replace("]","").replace("'","").replace("\"","")+","
        else:
            indexFile.write("\""+str(metadata_property)+"\" :"+ str([])+extrachar)
            CSVvalue=CSVvalue+","

#    value=(getValueHTML_SeaDataNet("Availability", datasetContents))
#    value=(getValueHTML_SeaDataNet("Ongoing", datasetContents))
#    value=(getValueHTML_SeaDataNet("Global identifier", datasetContents))
    indexFile.write("}")
    indexFile.close()
    Precision, Recall, Accuracy, F=  metadataRecord_similarity_evaluation(indexfname+".json",'EssentialVariables',['description','keywords','abstract'],essentialVariables)
    CSVvalue=CSVvalue+ str(Precision)+","+ str(Recall)+"," +str(Accuracy)+","+ str(F) +","
    Precision, Recall, Accuracy, F = metadataRecord_similarity_evaluation(indexfname+".json",'potentialTopics',['description','keywords','abstract'],str(lstKeywords).split(","))
    CSVvalue=CSVvalue+ str(Precision)+","+ str(Recall)+"," +str(Accuracy)+","+ str(F) +","
    logfile.write(CSVvalue)
    logfile.close()

#----------------------------------------------------------------------------------------
def NestedDictValues(d):
    if type(d)==dict:
        for v in d.values():
            if isinstance(v, dict):
                yield from NestedDictValues(v)
            else:
                yield v
#----------------------------------------------------------------------------------------
def MergeList(contextualText):
    lstText=[]
    for entity in contextualText:
        if type(entity)==list:
            for item in entity:
                lstText.append(str(item).strip())
        else:
            lstText.append(str(entity).strip())
    return lstText
#----------------------------------------------------------------------------------------
def getContextualText_SeaDataNet_CDI(JSON):
    contextualText=""
    contextualText=deep_search(["Data set name", "Discipline","Parameter groups","Discovery parameter","GEMET-INSPIRE themes"],JSON)
    if not len(contextualText):
        contextualText=deep_search(["Abstract"],JSON)
    contextualText=list(NestedDictValues(contextualText))
    return MergeList(contextualText)
#----------------------------------------------------------------------------------------
def getContextualText_SeaDataNet_EDMED(JSON):
    contextualText=""
    contextualText=deep_search(["name", "keywords","measurementTechnique"],JSON)
    if not len(contextualText):
        contextualText=deep_search(["abstract"],JSON)
    contextualText=list(NestedDictValues(contextualText))
    return MergeList(contextualText)
#----------------------------------------------------------------------------------------
def getContextualText_LifeWatch(JSON):
    contextualText=""
    contextualText=deep_search(["dataset", "title","abstract","citation","headline","publisher"],JSON)
    if not len(contextualText):
        contextualText=deep_search(["Abstract"],JSON)
    contextualText=list(NestedDictValues(contextualText))
    return MergeList(contextualText)
#----------------------------------------------------------------------------------------
def getContextualText_ICOS(JSON):
    contextualText=""
    contextualText=deep_search(["keywords", "genre","theme","name"],JSON)
    if not len(contextualText):
        contextualText=deep_search(["Abstract"],JSON)
    contextualText=list(NestedDictValues(contextualText))
    return MergeList(contextualText)
#----------------------------------------------------------------------------------------
def datasetProcessing_SeaDataNet_CDI(datasetURL):
    metadataStar_content = open(metadataStar_root,"r")
    metadataStar_object = json.loads(r''+metadataStar_content.read())
    with urllib.request.urlopen(datasetURL) as f:
        data = f.read().decode('utf-8')
    JSON=json.loads(r''+data)

    unique_filename = str(uuid.uuid4())
    indexfname = os.path.join(indexFiles_root,"SeaDataNet_CDI_"+unique_filename)
    indexFile= open(indexfname+".json","w+")

    logfile = os.path.join(indexFiles_root,"logfile.csv")
    CSVvalue=""
    if not os.path.exists(logfile):
        logfile= open(logfile,"a+")
        for metadata_property in metadataStar_object:
            CSVvalue=CSVvalue+metadata_property+","
        CSVvalue=CSVvalue+"Precision (EV), Recall (EV), Accuracy (EV), F (EV), Precision (To), Recall (To), Accuracy (To), F (To),"
        logfile.write(CSVvalue)
    else:
        logfile= open(logfile,"a+")

    CSVvalue="\n"

    indexFile.write("{\n")
    originalValues=[]
    RI=""
    domains=""
    topics=[]
    cnt=0
    lstKeywords=[]
    for metadata_property in metadataStar_object:
        cnt=cnt+1

        if metadata_property=="ResearchInfrastructure":
            result= getRI(JSON)
        elif metadata_property=="theme":
            if not len(RI):
                #RI= getRI(JSON)
                RI="SeaDataNet"
            if not len(domains):
                domains = getDomain(RI)
            if not len(topics):
                topics=topicMining(JSON,"SeaDataNet_CDI")
            result=getTopicsByDomainVocabulareis(topics,domains[0])
        elif metadata_property=="language":
            result="English"
        elif metadata_property=="potentialTopics":
            if not len(topics):
                topics=topicMining(JSON,"SeaDataNet_CDI")
            result=topics
            result= pruneExtractedContextualInformation(result, originalValues)
        elif metadata_property=="EssentialVariables":
            if not len(RI):
                RI= getRI(JSON)
            if not len(domains):
                domains = getDomain(RI)
            if not len(topics):
                topics=topicMining(JSON,"SeaDataNet_CDI")
            essentialVariables=getDomainEssentialVariables(domains[0])
            result=getSimilarEssentialVariables(essentialVariables,topics)
            result= pruneExtractedContextualInformation(result, originalValues)
        elif metadata_property=="url":
            result=datasetURL#[str(datasetURL)]
        elif metadata_property=="name":
            result=deep_search(["Data set name"],JSON)
        else:
            result=deep_search([metadata_property],JSON)
            if not len(result):
                searchFields=[]
                for i in range (3, len(metadataStar_object[metadata_property])):
                    result=deep_search([metadataStar_object[metadata_property][i]],JSON)
                    if len(result): searchFields.append(result)
                result=searchFields
        propertyDatatype=metadataStar_object[metadata_property][0]
        #if metadata_property!="url":
        result=refineResults(result,propertyDatatype,metadata_property)

        #if metadata_property=="language" and (result=="" or result==[]):
        #   result= LangaugePrediction(extractTextualContent(JSON))

        if(cnt==len(metadataStar_object)):
            extrachar="\n"
        else:
            extrachar=",\n"

        flattenValue=(str(MergeList(flatten_list(result)))
                          .replace("></a","").replace(",","-")
                          .replace("[","").replace("]","").replace("{","")
                          .replace("'","").replace("\"","").replace("}","")
                          .replace("\"\"","").replace(">\\","")
                          .replace("' ","'").replace(" '","'"))
        flattenValue= str([x.strip() for x in flattenValue.split('-')])

        if (metadata_property=="description" or metadata_property=="keywords" or metadata_property=="abstract"):
            txtVal=flattenValue.replace("[","").replace("]","").replace("'","").replace("\"","").replace("\"\"","").replace("None","")
            if txtVal!="":
                originalValues.append(txtVal)

        indexFile.write("\""+str(metadata_property)+"\" :"+flattenValue.replace("'","\"")+extrachar)
        CSVvalue=CSVvalue+flattenValue.replace(",","-").replace("[","").replace("]","").replace("'","").replace("\"","").replace("\"\"","")+","

        if metadata_property=="keywords":
            lstKeywords=flattenValue

    indexFile.write("}")
    indexFile.close()
    Precision, Recall, Accuracy, F=  metadataRecord_similarity_evaluation(indexfname+".json",'EssentialVariables',['description','keywords','abstract'],essentialVariables)
    CSVvalue=CSVvalue+ str(Precision)+","+ str(Recall)+"," +str(Accuracy)+","+ str(F) +","
    Precision, Recall, Accuracy, F = metadataRecord_similarity_evaluation(indexfname+".json",'potentialTopics',['description','keywords','abstract'],lstKeywords.split(","))
    CSVvalue=CSVvalue+ str(Precision)+","+ str(Recall)+"," +str(Accuracy)+","+ str(F) +","
    logfile.write(CSVvalue)
    logfile.close()

#----------------------------------------------------------------------------------------
class Decoder(json.JSONDecoder):
    def decode(self, s):
        result = super().decode(s)  # result = super(Decoder, self).decode(s) for Python 2.x
        return self._decode(result)

    def _decode(self, o):
        if isinstance(o, int):
            try:
                return str(o)
            except ValueError:
                return o
        elif isinstance(o, dict):
            return {k: self._decode(v) for k, v in o.items()}
        elif isinstance(o, list):
            return [self._decode(v) for v in o]
        else:
            return o
#----------------------------------------------------------------------------------------
def invertedIndexing(datasetTitle):
    indexfnames = os.path.join(indexFiles_root,datasetTitle)
    lstIndexFileNames=glob.glob(indexfnames+"*")
    Hashtablefnames = os.path.join(indexFiles_root,"Hashtable_"+datasetTitle+".csv")
    lstKeywords=[]
    lstEssentialVariables=[]
    lstPotentialTopics=[]
    cntCategory={}
    hashtable={}
    for indexFile in lstIndexFileNames:
        indexFile_content = open(indexFile,"r")
        indexFile_object = json.loads(r''+indexFile_content.read())
        lstKeywords.append(indexFile_object["keywords"])
        lstEssentialVariables.append(indexFile_object["EssentialVariables"])
        lstPotentialTopics.append(indexFile_object["potentialTopics"])

    lstKeywords=MergeList(lstKeywords)
    lstEssentialVariables=MergeList(lstEssentialVariables)
    lstPotentialTopics=MergeList(lstPotentialTopics)

    for keyword in lstKeywords:
        if len(keyword)<200 and len(keyword)>1:
            if keyword.lower() not in hashtable.keys():
                hashtable[keyword.lower()]=[]

    for keyword in lstEssentialVariables:
        if len(keyword)<200 and len(keyword)>1:
            if keyword.lower() not in hashtable.keys():
                hashtable[keyword.lower()]=[]

    for keyword in lstPotentialTopics:
        if len(keyword)<200 and len(keyword)>1:
            if keyword.lower() not in hashtable.keys():
                hashtable[keyword.lower()]=[]


    for indexFile in lstIndexFileNames:
        lstKeywords.clear()
        lstEssentialVariables.clear()
        lstPotentialTopics.clear()

        indexFile_content = open(indexFile,"r")
        indexFile_object = json.loads(r''+indexFile_content.read())
        lstKeywords.append(indexFile_object["keywords"])
        lstEssentialVariables.append(indexFile_object["EssentialVariables"])
        lstPotentialTopics.append(indexFile_object["potentialTopics"])
        if indexFile_object["url"]!=[]:
            url=indexFile_object["url"][0]
            url = re.findall("(?P<url>https?://[^\s]+)", url)
            if not len(url):
                continue
#        elif indexFile_object["otherLocale"]!=[]:
#            url=indexFile_object["otherLocale"][0]
        else:
            continue

        lstKeywords=MergeList(lstKeywords)
        lstEssentialVariables=MergeList(lstEssentialVariables)
        lstPotentialTopics=MergeList(lstPotentialTopics)

        for keyword in lstKeywords:
            if keyword.lower() in hashtable.keys():
                if url not in hashtable[keyword.lower()]:
                    hashtable[keyword.lower()].append(url)

        for keyword in lstEssentialVariables:
            if keyword.lower() in hashtable.keys():
                if url not in hashtable[keyword.lower()]:
                    hashtable[keyword.lower()].append(url)

        for keyword in lstPotentialTopics:
            if keyword.lower() in hashtable.keys():
                if url not in hashtable[keyword.lower()]:
                    hashtable[keyword.lower()].append(url)

    with open(Hashtablefnames, 'w') as f:
        for key in hashtable.keys():
            if str(len(hashtable[key])) not in cntCategory.keys():
                cntCategory[str(len(hashtable[key]))]=1
            else:
                cntCategory[str(len(hashtable[key]))]=cntCategory[str(len(hashtable[key]))]+1
        f.write("%s, %s, %s\n" % (currentRun, str(len(hashtable.keys())) , str(cntCategory)))
        for key in hashtable.keys():
            value= str(hashtable[key]).replace("'","").replace("[","").replace("]","")
            f.write("%s, %s, %s\n" % (str(len(hashtable[key])) , key, value))
    print("Inverted indexing is done!")
    f.close()

#----------------------------------------------------------------------------------------
def datasetProcessing_SeaDataNet_CDI_XML(datasetURL):
    metadataStar_content = open(metadataStar_root,"r")
    metadataStar_object = json.loads(r''+metadataStar_content.read())
    with urllib.request.urlopen(datasetURL) as f:
        data = f.read().decode('utf-8')
    data=data.replace("xml:","").replace("eml:","").replace("namespace:","").replace("xmlns:","").replace("gmd:","").replace("gco:","").replace("sdn:","").replace("gml:","").replace("gts:","").replace("xlink:","").replace("\"{","")
    xml = fromstring(data.encode())
    JSON=json.loads(r''+json.dumps(xmljson.badgerfish.data(xml)),cls=Decoder)
#----------------------------------------------------------------------------------------
def datasetProcessing_LifeWatch(datasetURL):
    metadataStar_content = open(metadataStar_root,"r")
    metadataStar_object = json.loads(r''+metadataStar_content.read())

    unique_filename = str(uuid.uuid4())
    indexfname = os.path.join(indexFiles_root,"LifeWatch_"+unique_filename)
    indexFile= open(indexfname+".json","w+")

    logfile = os.path.join(indexFiles_root,"logfile.csv")
    CSVvalue=""
    if not os.path.exists(logfile):
        logfile= open(logfile,"a+")
        for metadata_property in metadataStar_object:
            CSVvalue=CSVvalue+metadata_property+","
        CSVvalue=CSVvalue+"Precision (EV), Recall (EV), Accuracy (EV), F (EV), Precision (To), Recall (To), Accuracy (To), F (To),"
        logfile.write(CSVvalue)
    else:
        logfile= open(logfile,"a+")

    CSVvalue="\n"

    indexFile.write("{\n")
    originalValues=[]
    RI=""
    domains=""
    topics=[]
    cnt=0
    lstKeywords=[]
    datasetDic={}
    with urllib.request.urlopen(datasetURL) as f:
        data = f.read().decode('utf-8')
    xmlTree = fromstring(data.encode())
    elemList = []

    for elem in xmlTree.iter():
        strValue=str(elem.text).replace("\n","").strip()
        strKey=elem.tag

        if strKey not in datasetDic:
            datasetDic[strKey]= [strValue]
        else:
            if strValue not in datasetDic[strKey]:
                datasetDic[strKey].append(strValue)

        if(strValue==""):
            elemList.append(strKey)
        else:
            for elem in elemList:
                datasetDic[elem].remove('')
                datasetDic[elem].append(strValue)
            elemList.clear()
    JSON=datasetDic
    for metadata_property in metadataStar_object:
        cnt=cnt+1

        if metadata_property=="ResearchInfrastructure":
            result= "LifeWatch"
        elif metadata_property=="theme":
            if not len(RI):
                #RI= getRI(JSON)
                RI="LifeWatch"
            if not len(domains):
                domains = getDomain(RI)
            if not len(topics):
                topics=topicMining(JSON, "LifeWatch")
            result=getTopicsByDomainVocabulareis(topics,domains[0])
        elif metadata_property=="language":
            result="English"
        elif metadata_property=="potentialTopics":
            if not len(topics):
                topics=topicMining(JSON, "LifeWatch")
            result=topics
            result= pruneExtractedContextualInformation(result, originalValues)
        elif metadata_property=="EssentialVariables":
            if not len(RI):
                RI= getRI(JSON)
            if not len(domains):
                domains = getDomain(RI)
            if not len(topics):
                topics=topicMining(JSON, "LifeWatch")
            essentialVariables=getDomainEssentialVariables(domains[0])
            result=getSimilarEssentialVariables(essentialVariables,topics)
            result= pruneExtractedContextualInformation(result, originalValues)
        elif metadata_property=="url":
            result=datasetURL#[str(datasetURL)]
        else:
            result=deep_search([metadata_property],JSON)
            if not len(result):
                searchFields=[]
                for i in range (3, len(metadataStar_object[metadata_property])):
                    result=deep_search([metadataStar_object[metadata_property][i]],JSON)
                    if len(result): searchFields.append(result)
                result=searchFields
        propertyDatatype=metadataStar_object[metadata_property][0]
        #if metadata_property!="url":
        result=refineResults(result,propertyDatatype,metadata_property)
        #if metadata_property=="language" and (result=="" or result==[]):
        #   result= LangaugePrediction(extractTextualContent(JSON))

        if(cnt==len(metadataStar_object)):
            extrachar="\n"
        else:
            extrachar=",\n"

        flattenValue=(str(MergeList(flatten_list(result)))
                      .replace("></a","").replace(",","-")
                      .replace("[","").replace("]","").replace("{","")
                      .replace("'","").replace("\"","").replace("}","")
                      .replace("\"\"","").replace(">\\","")
                      .replace("' ","'").replace(" '","'"))
        flattenValue= str([x.strip() for x in flattenValue.split('-')])

        if (metadata_property=="description" or metadata_property=="keywords" or metadata_property=="abstract"):
            txtVal=flattenValue.replace("[","").replace("]","").replace("'","").replace("\"","").replace("\"\"","").replace("None","")
            if txtVal!="":
                originalValues.append(txtVal)

        indexFile.write("\""+str(metadata_property)+"\" :"+flattenValue.replace("'","\"")+extrachar)
        CSVvalue=CSVvalue+flattenValue.replace(",","-").replace("[","").replace("]","").replace("'","").replace("\"","").replace("\"\"","")+","

        if metadata_property=="keywords":
            lstKeywords=flattenValue

    indexFile.write("}")
    indexFile.close()

    Precision, Recall, Accuracy, F=  metadataRecord_similarity_evaluation(indexfname+".json",'EssentialVariables',['description','keywords','abstract'],essentialVariables)
    CSVvalue=CSVvalue+ str(Precision)+","+ str(Recall)+"," +str(Accuracy)+","+ str(F) +","   
    Precision, Recall, Accuracy, F = metadataRecord_similarity_evaluation(indexfname+".json",'potentialTopics',['description','keywords','abstract'], (lstKeywords.split(",")))
    CSVvalue=CSVvalue+ str(Precision)+","+ str(Recall)+"," +str(Accuracy)+","+ str(F) +","
    logfile.write(CSVvalue)
    logfile.close()
#----------------------------------------------------------------------------------------
def get_jaccard_sim(str1, str2):
    a = set(str1.split())
    b = set(str2.split())
    c = a.intersection(b)
    if ((len(a) + len(b) - len(c))>0) :
        sim = float(len(c)) / (len(a) + len(b) - len(c))
    else :
        sim=0
    return sim
#----------------------------------------------------------------------------------------
def getFalseNegative(drivedFields, originalFields, SetOfValues):
    lstPotentialValues=[]
    for orgFields in originalFields:
        for potentialvalue in SetOfValues:
            simScore=get_jaccard_sim(potentialvalue.lower(),orgFields.lower())
            if(simScore>CommonSubsetThreshold and potentialvalue not in drivedFields and potentialvalue not in lstPotentialValues):
                lstPotentialValues.append(potentialvalue)
    return lstPotentialValues
#----------------------------------------------------------------------------------------
def pruneExtractedContextualInformation(drivedValues, originalValues):
    #########################################
    # Turn it off:
    #return drivedValues
    ########################################
    lstAcceptedValues=[]
    if len(drivedValues) and len(originalValues):
        for subDrivedField in drivedValues:
            for originalField in originalValues:
                simScore=get_jaccard_sim(subDrivedField.lower(),originalField.lower())
                if(simScore> CommonSubsetThreshold and subDrivedField not in lstAcceptedValues):
                    lstAcceptedValues.append(subDrivedField)
    else:
        lstAcceptedValues=drivedValues

    return lstAcceptedValues
#----------------------------------------------------------------------------------------
def metadataRecord_similarity_evaluation(filename, drivedField, originalFields, SetOfPotentialValues):
    #--------------Turn off
    return 0,0,0,0
    #--------------

    dataset_content = open(filename,"r")
    dataset_object = json.loads(r''+dataset_content.read())

    similarityDic={}
    ScoreSim={}

    lstOriginalValues=[]
    lstdrivedValues=[]

    TruePositive=[]
    FalsePositive=[]
    FalseNegative=[]
    TrueNegative=[]

    for originalField in originalFields:
        for subDrivedField in dataset_object[drivedField]:
            for subOriginalField in dataset_object[originalField]:
                simScore=get_jaccard_sim(subDrivedField.lower(),subOriginalField.lower())

                if subOriginalField not in lstOriginalValues:
                    lstOriginalValues.append(subOriginalField)

                if subDrivedField not in lstdrivedValues:
                    lstdrivedValues.append(subDrivedField)

                if simScore>CommonSubsetThreshold and subDrivedField not in TruePositive:
                    TruePositive.append(subDrivedField)

    for drivedVal in dataset_object[drivedField]:
        if drivedVal not in TruePositive:
            FalsePositive.append(drivedVal)

    FalseNegative=getFalseNegative(lstdrivedValues, lstOriginalValues, SetOfPotentialValues)

    for PotentialVal in SetOfPotentialValues:
        if PotentialVal not in dataset_object[drivedField] and PotentialVal not in FalseNegative:
            TrueNegative.append(PotentialVal)

    TP=len(TruePositive)
    FP=len(FalsePositive)
    TN=len(TrueNegative)
    FN=len(FalseNegative)

    Precision=0
    Recall=0
    Accuracy=0
    F=0
    
    if (TP+FP)>0:
        Precision= TP / (TP+FP)
    if (TP+FN)>0:
        Recall= TP/(TP+FN)
    if(TP+TN+FP+FN)>0:
        Accuracy= (TP+ TN)/ (TP+TN+FP+FN)
    if(Precision + Recall)>0:
        F= 2 * (Precision*Recall)/ (Precision + Recall)

    return Precision, Recall, Accuracy, F
#----------------------------------------------------------------------------------------
def getCurrentListOfDatasetRecords(datasetTitle):
    RndIndexFiles = os.path.join(indexFiles_root,datasetTitle+".csv")
    lstIndexFileNames=[]
    with open(RndIndexFiles, newline='') as f:
        reader = csv.reader(f)
        for url in reader:
            lstIndexFileNames.append(url[0])
    f.close()
    return lstIndexFileNames
#----------------------------------------------------------------------------------------
def saveSelectedURLs(lstDataset, datasetTitle):
    RndIndexFiles = os.path.join(indexFiles_root,datasetTitle+".csv")
    with open(RndIndexFiles, 'w') as f:
        for url in lstDataset:
            f.write("%s\n" % url)
    f.close()
#--------------------
#----------------------------------------------------------------------------------------
# Create index if not exist with correct settings
# if index exists, change settings

def Run_indexingPipeline_ingest_indexFiles():
    es = Elasticsearch("http://localhost:9200")
    index = Index('envri', es)

    if not es.indices.exists(index='envri'):
        index.settings(
            index={'mapping': {'ignore_malformed': True}}
        )
        index.create()
    else:
        es.indices.close(index='envri')
        put = es.indices.put_settings(
            index='envri',
            body={
                "index": {
                    "mapping": {
                        "ignore_malformed": True
                    }
                }
            })
        es.indices.open(index='envri')

    # path is correct IF this file is in the same folder as 'envri_json'
    indexfnames = os.path.join(indexFiles_root)
    filelist=glob.glob(indexfnames+"*")

    indexed = 0
    for i in range(len(filelist)):
        doc = open_file(filelist[i])
        id = doc["url"]
        print(round(((i + 1) / len(filelist) * 100), 2), "%", filelist[i])  # keep track of progress / counter
        indexed += 1
        res = es.index(index="envri", id=doc["url"], body=doc)
        es.indices.refresh(index="envri")
    deleteAllIndexFilesByExtension(".json")
# ----------------------------------------------------------------
def open_file(file):
    read_path = file
    with open(read_path, "r", errors='ignore') as read_file:
        data = json.load(read_file)
    return data
# ----------------------------------------------------------------------
def Run_indexingPipeline_ICOS():
    print("indexing the ICOS dataset repository has been started!")
    getDatasetRecords__ICOS()
    cnt=1
    # ........................................
    lstDataset= getOnlineDatasetRecords__ICOS(False,10,1)
    for datasetURL in lstDataset:
        if not(if_URL_exist(datasetURL)):
            datasetProcessing_ICOS(datasetURL)
            print("\n ICOS ----> \n Record: "+'{0:.3g}'.format(cnt/len(lstDataset))+" % \n ----> \n")
        else :
            print(datasetURL)
        cnt=cnt+1
    # ........................................
        deleteAllIndexFilesByExtension(".csv")
        Run_indexingPipeline_ingest_indexFiles()
    print("The indexing process has been finished!")
# ----------------------------------------------------------------------
def Run_indexingPipeline_SeaDataNet_EDMED():
    print("indexing the SeaDataNet EDMED dataset repository has been started!")
    getDatasetRecords__SeaDataNet_EDMED()
    cnt=1
    # ........................................
    lstDataset= getOnlineDatasetRecords__SeaDataNet_EDMED(False,10,1)
    for datasetURL in lstDataset:
        if not(if_URL_exist(datasetURL)):
            datasetProcessing_SeaDataNet_EDMED(datasetURL)
            print("\n EDMED ----> \n Record: "+'{0:.3g}'.format(cnt/len(lstDataset))+" % \n ----> \n")
        else :
            print(datasetURL)
        cnt=cnt+1
    # ........................................
        deleteAllIndexFilesByExtension(".csv")
        Run_indexingPipeline_ingest_indexFiles()
    print("The indexing process has been finished!")
# ----------------------------------------------------------------------
def Run_indexingPipeline_SeaDataNet_CDI():
    print("indexing the SeaDataNet CDI dataset repository has been started!")
    getDatasetRecords__SeaDataNet_CDI()
    # ........................................
    cnt=1
    lstDataset= getOnlineDatasetRecords__SeaDataNet_CDI(False,10,1)
    for datasetURL in lstDataset:
        if not(if_URL_exist(datasetURL)):
            datasetProcessing_SeaDataNet_CDI(datasetURL)
            print("\n CDI ----> \n Record: "+'{0:.3g}'.format(cnt/len(lstDataset))+" % \n ----> \n")
        else :
            print(datasetURL)
        cnt=cnt+1
        # ........................................
        deleteAllIndexFilesByExtension(".csv")
        Run_indexingPipeline_ingest_indexFiles()
    print("The indexing process has been finished!")
# ----------------------------------------------------------------------
def Run_indexingPipeline_LifeWatch():
    print("indexing the SLifeWatch dataset repository has been started!")
    getDatasetRecords__LifeWatch()
    # ........................................
    lstDataset= getOnlineDatasetRecords__LifeWatch(False,1,1)
    for datasetURL in lstDataset:
        if not(if_URL_exist(datasetURL)):
            datasetProcessing_LifeWatch(datasetURL)
            print("\n LifeWatch ----> \n Record: "+str(cnt)+"\n ----> \n")
            cnt=cnt+1
        else :
            print(datasetURL)
    # ........................................
        deleteAllIndexFilesByExtension(".csv")
        Run_indexingPipeline_ingest_indexFiles()
    print("The indexing process has been finished!")
# ----------------------------------------------------------------------

def deleteAllIndexFilesByExtension(extension):
    directory = indexFiles_root
    files_in_directory = os.listdir(directory)
    filtered_files = [file for file in files_in_directory if file.endswith(extension)]
    for file in filtered_files:
        path_to_file = os.path.join(directory, file)
        os.remove(path_to_file)
# ----------------------------------------------------------------------
def if_URL_exist(url):
    es = Elasticsearch("http://localhost:9200")
    index = Index('envri', es)

    if not es.indices.exists(index='envri'):
        index.settings(
            index={'mapping': {'ignore_malformed': True}}
        )
        index.create()
    else:
        es.indices.close(index='envri')
        put = es.indices.put_settings(
            index='envri',
            body={
                "index": {
                    "mapping": {
                        "ignore_malformed": True
                    }
                }
            })
        es.indices.open(index='envri')

    user_request = "some_param"
    query_body = {
        "query": {
            "bool": {
                "must": [{
                    "match_phrase": {
                        "url": url
                    }
                }]
            }
        },
        "from": 0,
        "size": 1
    }
    result = es.search(index="envri", body=query_body)
    numHits=result['hits']['total']['value']
    return True if numHits>0 else False

#getDatasetRecords__SeaDataNet_EDMED()
#getDatasetRecords__SeaDataNet_CDI()
#getDatasetRecords__LifeWatch()
#getDataSetRecords__ICOS()
#--------------------
#lstDataset= getOnlineDatasetRecords__SeaDataNet_EDMED(True,100,1)
#lstDataset= getOnlineDatasetRecords__SeaDataNet_CDI(True,100,1)
#lstDataset= getOnlineDatasetRecords__ICOS(True,100,1)
#lstDataset= getOnlineDatasetRecords__LifeWatch(True,100,1)
#--------------------
#lstDataset= getCurrentListOfDatasetRecords("SeaDataNet_CDI")
#for datasetURL in lstDataset:
#    datasetProcessing_SeaDataNet_CDI(datasetURL)
#--------------------
#lstDataset= getCurrentListOfDatasetRecords("ICOS")
#for datasetURL in lstDataset:
#    datasetProcessing_ICOS(datasetURL)
#--------------------
#lstDataset= getCurrentListOfDatasetRecords("LifeWatch")
#for datasetURL in lstDataset:
#   datasetProcessing_LifeWatch(datasetURL)
#--------------------
#lstDataset= getCurrentListOfDatasetRecords("SeaDataNet_EDMED")
#for datasetURL in lstDataset:
#   datasetProcessing_SeaDataNet_EDMED(datasetURL)
#--------------------
#datasetProcessing_SeaDataNet_CDI("https://cdi.seadatanet.org/report/aggregation/120/2688/120/4/ds12/json")
#datasetProcessing_ICOS("https://meta.icos-cp.eu/objects/Msxml8TlWbHvmQmDD6EdVgPc")
#datasetProcessing_ICOS("https://meta.icos-cp.eu/objects/7c3iQ3A8SAeupVvMi8wFPWEN")
#datasetProcessing_SeaDataNet_EDMED("https://edmed.seadatanet.org/report/249/")
#datasetProcessing_LifeWatch("https://metadatacatalogue.lifewatch.eu/srv/api/records/oai:marineinfo.org:id:dataset:4040/formatters/xml?approved=true")
#--------------------
#invertedIndexing("SeaDataNet_CDI_")
#invertedIndexing("LifeWatch_")
#invertedIndexing("SeaDataNet_EDMED_")
#invertedIndexing("ICOS_")
#--------------------
deleteAllIndexFilesByExtension(".json")
deleteAllIndexFilesByExtension(".csv")
#--------------------
#Run_indexingPipeline_SeaDataNet_CDI()
#Run_indexingPipeline_SeaDataNet_EDMED()
Run_indexingPipeline_ICOS()

#--------------------
#datasetProcessing_ICOS("https://meta.icos-cp.eu/objects/0ST81nXCND5VfAQdOCSJDveT")
