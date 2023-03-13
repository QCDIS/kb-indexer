from typing import List

from elasticsearch.exceptions import AuthorizationException

from indexers import utils


indices_sub_groups = {
    'dataset': ('repo', [
        'DiSSCo',
        'ICOS',
        # 'LifeWatch',
        'SeaDataNet',
        'SIOS',
        ]),
    }


def count_match(es_client, index, keyword, term):
    res = es_client.count(
        index=index,
        body={
            'query': {
                'match': {
                    keyword: term,
                    }
                }
            }
        )
    return res['count']


def list_indices(es_client):
    indices = es_client.indices.get_alias(index='*')
    if not indices:
        print('No indices found')
    for index_name in sorted(indices):
        if index_name.startswith('.'):
            continue
        try:
            es_client.indices.open(index=index_name)
            print(
                index_name,
                es_client.count(index=index_name)["count"],
                )
            if index_name in indices_sub_groups:
                keyword, terms = indices_sub_groups[index_name]
                for term in terms:
                    print(
                        ' ',
                        term,
                        count_match(es_client, index_name, keyword, term)
                        )
        except AuthorizationException as e:
            print(f'could not open index {index_name} ({e})')


if __name__ == '__main__':
    es = utils.create_es_client()
    if es is None:
        raise ValueError('Could not connect to elasticsearch')

    list_indices(es)
