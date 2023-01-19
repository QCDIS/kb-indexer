from typing import List

from indexers import utils


def list_indices(es):
    indices = es.indices.get_alias(index='*')
    if not indices:
        print('No indices found')
    for index_name in indices:
        es.indices.open(index=index_name)
        print(
            index_name,
            es.count(index=index_name)["count"],
            )


class _GenericSearch:

    index_name: str
    search_fields: List[str]
    print_fields: List[str]

    def __init__(self, es):
        self.es = es

    def print_results(self, results):
        if results['hits']['total']['value'] == 0:
            print('No results')
        for hit in results['hits']['hits']:
            source = hit['_source']
            print(*(source[k] for k in self.print_fields))

    def query(self, query, print_results):
        query_body = {
            "from": 0,
            "size": 200,
            "query": {
                "bool": {
                    "must": {}
                    }
                }
            }
        if not query:
            query_body["query"]["bool"]["must"]["match_all"] = {}
        else:
            query_body["query"]["bool"]["must"]["multi_match"] = {
                "query": query,
                "fields": self.search_fields,
                # "type": "best_fields",
                # "minimum_should_match": "50%"
                }
        results = self.es.search(index=self.index_name, body=query_body)
        if print_results:
            self.print_results(results)
        return results


class APISearch(_GenericSearch):
    index_name = 'webapi'
    search_fields = ['name', 'description', 'category', 'provider',
                     'serviceType', 'architecturalStyle']
    print_fields = ['name', 'url']


class KaggleSearch(_GenericSearch):
    index_name = 'kaggle_notebooks'
    search_fields = ['name', 'description']
    print_fields = ['html_url', 'name']


class RawKaggleSearch(_GenericSearch):
    index_name = 'kaggle_raw_notebooks'
    search_fields = ['name', 'description']
    print_fields = ['docid', 'name']


class WebSearch(_GenericSearch):
    index_name = 'webcontents'
    search_fields = ['title', 'file']
    print_fields = ['url', 'title']


class DatasetSearch(_GenericSearch):
    index_name = 'envri'
    search_fields = ['name', 'description']
    print_fields = ['url', 'name']


if __name__ == '__main__':
    es = utils.create_es_client()
    if es is None:
        raise ValueError('Could not connect to elasticsearch')

    list_indices(es)

    # r = WebSearch(es).query('', print_results=True)
    # r = DatasetSearch(es).query('', print_results=True)
    # r = KaggleSearch(es).query('GANs with Keras', print_results=True)
    # r = RawKaggleSearch(es).query('', print_results=True)
    # r = APISearch(es).query('', print_results=True)
