import os


class Paths:
    def __init__(self, repo_name: str):
        data_dir = self._add_dir(os.path.dirname(__file__), 'data')

        # ancillary data
        self.nltk_data_dir = self._add_dir(data_dir, 'nltk_data')

        # repository data
        repo_dir = self._add_dir(data_dir, repo_name)
        self.index_records_dir = self._add_dir(
            repo_dir, 'index_records')
        self.dataset_list_filename = os.path.join(
            repo_dir, f'dataset_list.xxx')  # FIXME
        self.dataset_urls_filename = os.path.join(
            repo_dir, f'dataset_urls.txt')

        # metadata
        metadata_dir = os.path.join(os.path.dirname(__file__), 'data_sources')
        self.metadataStar_filename = os.path.join(
            metadata_dir, 'metadata*.json')
        self.RI_filename = os.path.join(
            metadata_dir, 'RIs.json')
        self.domain_filename = os.path.join(
            metadata_dir, 'domain.json')
        self.essentialVariabels_filename = os.path.join(
            metadata_dir, 'essential_variables.json')
        self.domainVocbularies_filename = os.path.join(
            metadata_dir, 'Vocabularies.json')

    @staticmethod
    def _add_dir(*path):
        dirname = os.path.join(*path)
        os.makedirs(dirname, exist_ok=True)
        return dirname
