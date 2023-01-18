from elasticsearch_dsl import Index
import os
import json
import uuid

from .. import utils

def open_file(file):
    read_path = file
    with open(read_path, "r", errors='ignore') as read_file:
        print(read_path)
        data = json.load(read_file)
        return data


def indexing_pipeline():
    indexer = utils.ElasticsearchIndexer('webapi')

    root = os.path.join(os.path.dirname(__file__), 'data_sources/DB')
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

            indexer.ingest_record(uuid.uuid4(), newRecord)


if __name__ == '__main__':
    indexing_pipeline()
