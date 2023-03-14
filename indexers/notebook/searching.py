import abc
import os
import re
import pandas as pd
import kaggle
from kaggle.rest import ApiException
from github import Github
import json
import time
from typing import List
from tqdm import tqdm
from urllib.parse import quote


class NotebookSearcher(abc.ABC):

    source_name: str

    def __init__(self):
        data_dir = os.getenv('DATA_DIR', '/kb-indexer-data')
        data_dir = os.path.join(data_dir, 'notebook')
        base = os.path.join(data_dir, self.source_name)
        self.output_dir = os.path.join(base, 'repositories_metadata/')
        os.makedirs(self.output_dir, exist_ok=True)

    @abc.abstractmethod
    def search(self, query: str, page_range: int) -> List[dict]:
        """ Search a given query term

        :param query: search query
        :param page_range: number of pages to crawl for each query
        :return: list of results metadata, containing at least a 'id' entry.
        """
        pass

    def bulk_search(self, queries: List[str], page_range: int):
        """ Loop over queries and save results to disk

        :param queries: list of search queries
        :param page_range: number of pages to crawl for each query
        """
        for i, query in enumerate(queries):
            print(f'[Notebook search] query {i}/{len(queries)}: {query}')
            results = self.search(query, page_range)
            for result in tqdm(results, 'saving search results'):
                output_file = os.path.join(
                    self.output_dir, f"{result['id']}.json")
                if not os.path.isfile(output_file):
                    with open(output_file, 'w') as f:
                        json.dump(result, f)
                else:
                    print(f'file exists: {output_file}')


class KaggleNotebookSearcher(NotebookSearcher):

    source_name = 'Kaggle'

    def search(self, query, page_range):
        kernels = []
        for page in range(1, page_range + 1):
            try:
                kernel_list = kaggle.api.kernels_list(search=query, page=page)
                if len(kernel_list) == 0:
                    break
                else:
                    kernels.extend(kernel_list)
            # Skip the pages that cause ApiException
            except ApiException as e:
                print('Exception encountered:', e)
                continue

        # Extract the `title` and the `ref` of returned Kaggle kernels
        results = []
        for kernel in kernels:
            if not kernel.ref:
                # happens with private notebooks
                continue
            results.append({
                'id': quote(kernel.ref, safe=''),
                'name': kernel.ref,
                'stargazers_count': kernel.totalVotes,
                'forks_count': None,
                'description': kernel.title,
                'size': None,
                'language': kernel.language,
                'html_url': 'https://www.kaggle.com/code/' + kernel.ref,
                'git_url': 'https://www.kaggle.com/code/' + kernel.ref,
                'source': self.source_name,
                'query': query,
                })

        return results


class GithubNotebookSearcher(NotebookSearcher):

    source_name = 'GitHub'

    def __init__(self):
        super().__init__()
        self._init_api_token()

    def _init_api_token(self):
        self._api_token = os.environ['GITHUB_API_TOKEN']

    @staticmethod
    def _wait_for_available_search_rate(g):
        while g.get_rate_limit().search.remaining <= 0:
            print('API rate exceeded, waiting')
            time.sleep(10)

    def search(self, query, page_range):
        query += ' language:jupyter-notebook'
        query += ' in:readme in:description in:topics'

        g = Github(self._api_token)
        self._wait_for_available_search_rate(g)
        result = g.search_repositories(
            query,
            sort='stars',
            order='desc',
            )
        data = []
        results_per_page = 30  # from API documentation
        self._wait_for_available_search_rate(g)
        results_count = result.totalCount
        pages_with_results = (
            (results_count // results_per_page)
            + int(bool(results_count % results_per_page))
            )
        for page in range(min(page_range, pages_with_results)):
            self._wait_for_available_search_rate(g)
            page_result = result.get_page(page)
            for i, repo in enumerate(page_result):
                new_record = {
                    'id': quote(repo.full_name, safe=''),
                    'name': repo.full_name,
                    'stargazers_count': repo.stargazers_count,
                    'forks_count': repo.forks_count,
                    'description': (re.sub(
                        r'[^A-Za-z0-9 ]+', '', repo.description) if
                        repo.description is not None else None),
                    'size': repo.size,
                    'language': repo.language,
                    'html_url': repo.html_url,
                    'git_url': repo.clone_url,
                    'query': query,
                    }
                if new_record not in data:
                    data.append(new_record)
        return data
