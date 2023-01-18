import os
import json
import time
from elasticsearch import Elasticsearch
import dotenv


# Ignore warnings
import warnings
from elasticsearch import ElasticsearchWarning
warnings.simplefilter('ignore', ElasticsearchWarning)


def read_json_file(file):
    read_path = file
    with open(read_path, "r", errors='ignore') as read_file:
        data = json.load(read_file)
    return data


def create_es_client() -> Elasticsearch:
    """ Create an Elasticsearch client based on the IP addresses/hostname.
    Returns:
        es: an elasticsearch client.

    """
    dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
    elasticsearch_hostname = os.environ.get('ELASTICSEARCH_HOSTNAME')
    elasticsearch_port = os.environ.get('ELASTICSEARCH_PORT')
    elasticsearch_username = os.environ.get('ELASTICSEARCH_USERNAME')
    elasticsearch_password = os.environ.get('ELASTICSEARCH_PASSWORD')

    es = Elasticsearch(
        hosts=[{"host": elasticsearch_hostname, "port": elasticsearch_port}],
        http_auth=[elasticsearch_username, elasticsearch_password],
        tim_out=30
        )

    # Try to reconnect to Elasticsearch for 10 times when failing
    # This is useful when Elasticsearch service is not fully online,
    # which usually happens when starting all services at once.
    for i in range(50):
        if not es.ping():
            time.sleep(1)
            print('[Elasticsearch] waiting for server')
            continue
        else:
            break
    if not es.ping():
        raise ValueError('[Elasticsearch] could not connect to server')
    print(f'[Elasticsearch] connected to {elasticsearch_hostname}')
    return es
