import os
import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import colorama
import json
import time
import urllib3
import re
from datetime import datetime
import spacy
from spacy import displacy
from collections import Counter
import en_core_web_sm
import lxml.html
import validators
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Index
nlp = en_core_web_sm.load()
#-----------------------------------------------------------------------------------------------------------------------
# init the colorama module
colorama.init()
GREEN = colorama.Fore.GREEN
GRAY = colorama.Fore.LIGHTBLACK_EX
RESET = colorama.Fore.RESET
YELLOW = colorama.Fore.YELLOW
# initialize the set of links (unique links)
internal_urls = set()
external_urls = set()
permitted_urls=set()
urllib3.disable_warnings()
#-----------------------------------------------------------------------------------------------------------------------
max_urls=1
config={}
HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36',}
# number of urls visited so far will be stored here
total_urls_visited = 0

data_dir = os.path.dirname(__file__)

extensionList = open(os.path.join(data_dir, 'data_sources/extensions.json'), "r")
extensionList = json.loads(r''+extensionList.read())

ResearchInfrastructures=open(os.path.join(data_dir,
                                          'data_sources/ResearchInfrastructures.json'
                                          ), "r")
ResearchInfrastructures = json.loads(r''+ResearchInfrastructures.read())

#-----------------------------------------------------------------------------------------------------------------------
def is_valid(url):
    """
    Checks whether `url` is a valid URL.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)
#-----------------------------------------------------------------------------------------------------------------------
def get_all_website_links(url):

    """
    Returns all URLs that is found on `url` in which it belongs to the same website
    """
    # all URLs of `url`
    urls = set()
    # domain name of the URL without the protocol
    domain_name = urlparse(url).netloc
    soup=""
    cnt=0
    while soup=='':
        try:
            soup = BeautifulSoup(requests.get(url,verify=True, timeout=5, headers=HEADERS).content, "html.parser",from_encoding="iso-8859-1")
            break
        except:
            print("Connection refused by the server...")
            time.sleep(0.2)
            cnt=cnt+1

            if cnt==20:
                return urls
            continue
    cnt=0
    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        if href == "" or href is None:
            # href empty tag
            continue

        # join the URL if it's relative (not absolute link)
        href = urljoin(url, href)
        parsed_href = urlparse(href)
        # remove URL GET parameters, URL fragments, etc.
        href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
        if not is_valid(href):
            # not a valid URL
            continue
        if href in internal_urls:
            # already in the set
            continue
        if domain_name not in href:
            continue
            # external link
            if href not in external_urls:
                print(f"{GRAY}[!] External link: {href}{RESET}")
                external_urls.add(href)
            continue
        urls.add(href)
        internal_urls.add(href)
        if href not in permitted_urls:
            permitted_urls.add(href)
            print(f"{GREEN}[*] Internal link: {href}{RESET}")
    return urls
#-----------------------------------------------------------------------------------------------------------------------
def extractHTML(url):
    soup=""
    cnt=0
    while soup=='':
        try:
            soup = BeautifulSoup(requests.get(url,verify=True, timeout=5, headers=HEADERS).content, "html.parser",from_encoding="iso-8859-1")
            break
        except:
            print("Connection refused by the server...")
            time.sleep(0.2)
            cnt=cnt+1

            if cnt==20:
                break
            continue
    if len(soup)>0 and len(soup.find_all('body'))>0:
        return soup
    else:
        return ""
#-----------------------------------------------------------------------------------------------------------------------
def extractFiles(url, html):
    global extensionList
    invalidExtensions=['com', 'js', 'css', 'php', 'asp', 'aspx', 'html', 'htm', 'net', 'org', 'edu',
                       'nl','it', 'fr', 'de', 'au' ]
    invalidSymbols=['/','#', '%', '?']

    fileList = []
    urls=set()
    for a_tag in html.findAll("a"):
        href = a_tag.attrs.get("href")

        if href == "" or href is None:
            # href empty tag
            continue

        domain = urlparse(href).netloc
        domain_ext=domain.split(".")[-1]
        href_ext=href.split(".")[-1]

        if (href_ext==domain_ext) or (href_ext in invalidExtensions):
            continue

        isinvalidSymbol=False
        for invalidSym in invalidSymbols:
            if invalidSym in href_ext:
                isinvalidSymbol=True

        if isinvalidSymbol:
            continue

        # join the URL if it's relative (not absolute link)
        href = urljoin(url, href)
        parsed_href = urlparse(href)
        # remove URL GET parameters, URL fragments, etc.
        href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
        if not is_valid(href):
            # not a valid URL
            continue
        if href in urls:
            # already in the set
            continue
        try:
            fileMetadata={
                'url': href,
                'extension': href_ext.lower(),
                'description': extensionList[href_ext.upper()]['descriptions']
            }
            fileList.append(fileMetadata)
        except:
            continue
    return fileList
#-----------------------------------------------------------------------------------------------------------------------
def extractTitle(html):
    lstTitle=[]
    for title in html.find_all('title'):
        lstTitle.append(title.get_text())
    return lstTitle

#-----------------------------------------------------------------------------------------------------------------------
def extractImages(url, html):
    imagelist =[]
    for img_tag in html.findAll("img"):
        src = img_tag.attrs.get("src")
        if src == "" or src is None:
            # src empty tag
            continue
        # join the URL if it's relative (not absolute link)
        src = urljoin(url, src)
        parsed_src = urlparse(src)
        # remove URL GET parameters, URL fragments, etc.
        src = parsed_src.scheme + "://" + parsed_src.netloc + parsed_src.path
        if not is_valid(src):
            # not a valid URL
            continue
        if src in imagelist:
            continue

        if(len(src)>300):
            continue

        imagelist.append(src)
    return imagelist

#-----------------------------------------------------------------------------------------------------------------------
def crawl(url):
    global max_urls
    invalidSigns=[]
    links=set()
    global total_urls_visited
    total_urls_visited += 1

    lenFolders=url.split('/')
    if( (len(url)<1024) and
            ('emso.eu/events/' not in url)  and
            ("mediawiki.envri.eu/index.php/Special:WhatLinksHere/" not in url) and
            ("mediawiki.envri.eu/index.php/Special:RecentChangesLinked/" not in url) ):
        print(f"{YELLOW}[*] Crawling: {url}{RESET}")
        links = get_all_website_links(url)
        for link in links:
            if total_urls_visited > max_urls:
                break
            crawl(link)
#-----------------------------------------------------------------------------------------------------------------------
def runCrawler(website):
    global config

    permitted_urls.clear()
    internal_urls.clear()
    external_urls.clear()

    crawl(website)
#-----------------------------------------------------------------------------------------------------------------------
def printResults():
    print("[+] Total Internal links:", len(internal_urls))
    print("[+] Total External links:", len(external_urls))
    print("[+] Total Positions links:", len(permitted_urls))
    print("[+] Total URLs:", len(external_urls) + len(internal_urls))
    print("[+] Total crawled URLs:", max_urls)
#-----------------------------------------------------------------------------------------------------------------------
def strippedText(txt):
    #invalidtext=["b3d33ef8a1f43935895d9f0b718ebe394f0e48f90bec8a95c15025dd14088c63", "window.dataLayer"]

    if type(txt)!=str:
        return ''

    cntNumbers = sum(c.isdigit() for c in txt)
    cntLetters = sum(c.isalpha() for c in txt)

    lstsplitdot=len(txt.split('.'))
    lstsplitcomma=len(txt.split(','))
    lstsplitspace=len(txt.split(' '))
    lstsplitsemicolon=len(txt.split(';'))

    lensign=lstsplitdot+lstsplitcomma+lstsplitsemicolon

    if(lstsplitspace<lensign) or (cntNumbers>(cntLetters/3)):
        return ''

    clean = re.compile('<.*?>')
    txt=re.sub(clean, '', txt)

    #for invalidStr in invalidtext:
    #    if invalidStr in txt:
    #        return ''

    if type(txt)==str:
        res = isinstance(txt, str)
        if res:
            txt=re.sub(r'[^A-Za-z0-9 .-\?/:,;~%$#*@!&+=_><]+', '', txt)
    if len(txt)==1:
        txt=""

    return txt
#-----------------------------------------------------------------------------------------------------------------------
def processContents(text):
    if len(text)>50000:
        text=text[:50000]
    pageContent=""
    pageSentences=[]

    for string in text.splitlines():
        txt=strippedText(string)
        if txt!='' and  txt!=' ' and (not validators.url(txt)) and (txt not in pageSentences):
            pageContent=pageContent+"\n"+txt
            pageSentences.append(txt)


    doc=nlp(pageContent)
    topics=[]
    dates=[]
    people=[]
    organizations=[]
    locations=[]
    products=[]
    workOfArt=[]
    items=[]

    for entitiy in doc.ents:
        text= strippedText(entitiy.text)

        if text!='':
            items.append(text)

            if entitiy.label_=="PERSON" and text not in people:
                people.append(text)
            elif entitiy.label_=="ORG" and text not in organizations:
                organizations.append(text)
            elif entitiy.label_=="DATE" and text not in dates:
                dates.append(text)
            elif entitiy.label_=="GPE"  and text not in locations:
                locations.append(text)
            elif entitiy.label_=="PRODUCT" and text not in products:
                products.append(text)
            elif entitiy.label_=="WORK_OF_ART" and text not in workOfArt:
                workOfArt.append(text)

        topics = [a_tuple[0] for a_tuple in Counter(items).most_common(20) if a_tuple[0].isalnum() and not a_tuple[0].isnumeric()]

    docFeatures={
        "topics":topics,
        "dates":dates,
        "people":people,
        "organizations":organizations,
        "locations":locations,
        "products":products,
        "workOfArt":workOfArt,
        "pageContetnts": sorted(pageSentences,key=len, reverse=True)
    }
    return docFeatures
#-----------------------------------------------------------------------------------------------------------------------
def remove_tags(raw_html):
    text= BeautifulSoup(raw_html, "lxml").text
    text= "\n".join([s for s in text.split("\n") if s])
    return text
#-----------------------------------------------------------------------------------------------------------------------
def getResearchInfrastructure(url):
    lstRI=[]
    for RI in ResearchInfrastructures:
        if RI in url:
            if(ResearchInfrastructures[RI]['acronym'] not in lstRI):
                lstRI.append(ResearchInfrastructures[RI])
    return lstRI
#-----------------------------------------------------------------------------------------------------------------------
def indexWebpage(url):
    html=extractHTML(url)

    if html!="":
        strippedHTML=remove_tags(str(html))
        metadata1=processContents(strippedHTML)
        metadata2={
            'url':[url],
            'title': extractTitle(html),
            'images': extractImages(url,html),
            'files': extractFiles(url,html),
            'researchInfrastructure': getResearchInfrastructure(url),
            'lastIndexingDate':[datetime.now()]
        }
        metadata= {**metadata1, **metadata2}
        return metadata
    else:
        return {}
#-----------------------------------------------------------------------------------------------------------------------
def indexWebsite(url):
    runCrawler(url)
    cnt=0
    print("-------------------")
    for url in permitted_urls:
        if not(if_URL_exist(url)):
            metadata=indexWebpage(url)

            if(metadata):
                print(url+"\n")
                ingest_metadataFile(metadata)
                cnt=cnt+1
                print("Metadata ingested ("+str(cnt)+")\n")
#-----------------------------------------------------------------------------------------------------------------------
def if_URL_exist(url):
    es = Elasticsearch("http://localhost:9200")
    index = Index('webcontents', es)

    if not es.indices.exists(index='webcontents'):
        index.settings(
            index={'mapping': {'ignore_malformed': True}}
        )
        index.create()
    else:
        es.indices.close(index='webcontents')
        put = es.indices.put_settings(
            index='webcontents',
            body={
                "index": {
                    "mapping": {
                        "ignore_malformed": True
                    }
                }
            })
        es.indices.open(index='webcontents')

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
    result = es.search(index="webcontents", body=query_body)
    numHits=result['hits']['total']['value']
    return True if numHits>0 else False
#-----------------------------------------------------------------------------------------------------------------------
def ingest_metadataFile(metadataFile):
    es = Elasticsearch("http://localhost:9200")
    index = Index('webcontents', es)

    if not es.indices.exists(index='webcontents'):
        index.settings(
            index={'mapping': {'ignore_malformed': True}}
        )
        index.create()
    else:
        es.indices.close(index='webcontents')
        put = es.indices.put_settings(
            index='webcontents',
            body={
                "index": {
                    "mapping": {
                        "ignore_malformed": True
                    }
                }
            })
        es.indices.open(index='webcontents')

        id = metadataFile["url"]
        res = es.index(index="webcontents", id=id, body=metadataFile)
        es.indices.refresh(index="webcontents")

#-----------------------------------------------------------------------------------------------------------------------
def envriCrawler():
    counter=1
    for IR in ResearchInfrastructures:
        if counter<27:
            counter=counter+1
            continue
        internal_urls.clear()
        external_urls.clear()
        permitted_urls.clear()
        total_urls_visited=0

        url= ResearchInfrastructures[IR]['url']
        indexWebsite(url)

    #url="http://mediawiki.envri.eu/index.php/Special:WhatLinksHere/%C3%83%C2%90%C3%82%C2%9C%C3%83%C2%90%C3%82%C2%BE%C3%83%C2%90%C3%82%C2%B1%C3%83%C2%90%C3%82%C2%B8%C3%83%C2%90%C3%82%C2%BB%C3%83%C2%91%C3%82%C2%8C%C3%83%C2%90%C3%82%C2%BD%C3%83%C2%90%C3%82%C2%B0%C3%83%C2%91%C3%82%C2%8F_%C3%83%C2%90%C3%82%C2%B2%C3%83%C2%90%C3%82%C2%B5%C3%83%C2%91%C3%82%C2%80%C3%83%C2%91%C3%82%C2%81%C3%83%C2%90%C3%82%C2%B8%C3%83%C2%91%C3%82%C2%8F_%C3%83%C2%90%C3%82%C2%BE%C3%83%C2%90%C3%82%C2%BD%C3%83%C2%90%C3%82%C2%BB%C3%83%C2%90%C3%82%C2%B0%C3%83%C2%90%C3%82%C2%B9%C3%83%C2%90%C3%82%C2%BD_%C3%83%C2%90%C3%82%C2%BA%C3%83%C2%90%C3%82%C2%B0%C3%83%C2%90%C3%82%C2%B7%C3%83%C2%90%C3%82%C2%B8%C3%83%C2%90%C3%82%C2%BD%C3%83%C2%90%C3%82%C2%BE_%C3%83%C2%90%C3%82%C2%92%C3%83%C2%91%C3%82%C2%83%C3%83%C2%90%C3%82%C2%BB%C3%83%C2%90%C3%82%C2%BA%C3%83%C2%90%C3%82%C2%B0%C3%83%C2%90%C3%82%C2%BD"
    #print(validators.url(url))
#-----------------------------------------------------------------------------------------------------------------------


def index_all_research_infrastructures():
    for IR in ResearchInfrastructures:
        internal_urls.clear()
        external_urls.clear()
        permitted_urls.clear()
        total_urls_visited=0

        url= ResearchInfrastructures[IR]['url']
        indexWebsite(url)


if __name__ == "__main__":
    #printResults()
    # envriCrawler()
    index_all_research_infrastructures()
#-----------------------------------------------------------------------------------------------------------------------



