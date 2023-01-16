from elasticsearch import Elasticsearch
from elasticsearch_dsl import Index
import os
import json
import uuid


def open_file(file):
    read_path = file
    with open(read_path, "r", errors='ignore') as read_file:
        print(read_path)
        data = json.load(read_file)
        return data


def indexing_pipeline():
    es = Elasticsearch("http://localhost:9200")
    index = Index('webapi', es)

    if not es.indices.exists(index='webapi'):
        index.settings(
            index={'mapping': {'ignore_malformed': True}}
            )
        index.create()
    else:
        es.indices.close(index='webapi')
        es.indices.put_settings(
            index='webapi',
            body={
                "index": {
                    "mapping": {
                        "ignore_malformed": True
                        }
                    }
                }
            )
        es.indices.open(index='webapi')

    root = os.path.join(os.path.dirname(__file__), 'DB')
    for path, _, files in os.walk(root):
        for name in files:
            record = os.path.join(path, name)
            record = open_file(record)
            newRecord = {
                "name": record["API name"],
                "description": record["Description"],
                "url": record["Url"],
                "category": record["Category"],
                "provider": record["Provider"],
                "serviceType": record["ServiceType"],
                "documentation": record["Documentation"],
                "architecturalStyle": record["Architectural Style"],
                "endpointUrl": record["Endpoint Url"],
                "sslSupprt": record["Support SSL"],
                "logo": record["Logo"]
                }

            es.index(index="webapi", id=uuid.uuid4(), body=newRecord)
            es.indices.refresh(index="webapi")


if __name__ == '__main__':
    indexing_pipeline()
