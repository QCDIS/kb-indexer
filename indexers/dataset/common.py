import os

from .. import utils


class Paths:
    def __init__(self, repo_name: str):
        data_dir = self._add_dir(
            os.path.relpath(os.path.dirname(__file__)),
            'data',
            )

        # ancillary data
        self.nltk_data_dir = self._add_dir(data_dir, 'nltk_data')

        # repository data
        repo_dir = self._add_dir(data_dir, repo_name)
        self.raw_dir = self._add_dir(repo_dir, 'raw')
        self.meta_dir = self._add_dir(repo_dir, 'metadata')
        self.converted_dir = self._add_dir(repo_dir, 'converted')

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

    @staticmethod
    def url_to_id(url):
        return utils.gen_id_from_url(url)

    @staticmethod
    def filename_to_id(filename):
        return os.path.splitext(os.path.basename(filename))[0]

    def raw_file(self, id_, ext):
        return os.path.join(self.raw_dir, id_ + ext)

    def meta_file(self, id_):
        return os.path.join(self.meta_dir, id_ + '.json')

    def converted_file(self, id_):
        return os.path.join(self.converted_dir, id_ + '.json')
