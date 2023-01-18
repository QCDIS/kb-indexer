import os
import json
import time
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Index
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


class ElasticsearchIndexer:

    def __init__(self, index_name):
        self.es = create_es_client()
        self.index_name = index_name
        self.index = self.initialize_index(self.index_name)

    @staticmethod
    def _apply_index_settings(index):
        index.settings(
            index={'mapping': {'ignore_malformed': True}}
            )

    def initialize_index(self, index_name):
        index = Index(index_name, self.es)
        if not index.exists():
            self._apply_index_settings(index)
            index.create()
        else:
            index.close()
            self._apply_index_settings(index)
            index.open()
        return index

    def is_in_index(self, field_name, value) -> bool:
        """ Check if the index contains an entry with <field_name> = <value>

        :param field_name: name of the field to check
        :param value: value to match
        :return: True if an entry is found, False otherwise
        """
        query = {
            "query": {
                "bool": {
                    "must": [{
                        "match_phrase": {
                            field_name: value,
                            }
                        }]
                    }
                },
            "from": 0,
            "size": 1
            }
        response = self.es.search(
            index=self.index_name,
            body=query,
            )
        num_hits = response['hits']['total']['value']
        return num_hits > 0

    def ingest_record(self, record_id: str, record: dict):
        """ Index record into elasticsearch

        :param record: record to index
        :param record_id: id of the index entry
        """
        self.es.index(
            index=self.index_name,
            id=record_id,
            body=record,
            )
        self.index.refresh()
