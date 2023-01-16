import os
import json
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
    valid_es = None
    dotenv.load_dotenv(os.path.join(os.getcwd(), '.env'))
    elasticsearch_hostname = os.environ.get('ELASTICSEARCH_HOSTNAME')
    elasticsearch_port = os.environ.get('ELASTICSEARCH_PORT')
    elasticsearch_username = os.environ.get('ELASTICSEARCH_USERNAME')
    elasticsearch_password = os.environ.get('ELASTICSEARCH_PASSWORD')

    es = Elasticsearch(
        hosts=[{"host": elasticsearch_hostname, "port": elasticsearch_port}],
        http_auth=[elasticsearch_username, elasticsearch_password],
        tim_out=30
        )

    connected = es.ping()
    if connected:
        print(f'\n[Elasticsearch] `{elasticsearch_hostname}` being connected\n')
        valid_es = es
    if valid_es == None:
        print(f'\n[Elasticsearch] data server is NOT ready yet!\n')
        # If there is no connection.
    return valid_es
