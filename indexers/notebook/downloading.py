import abc
import os
import pandas as pd
import kaggle
import json


class _NotebookDownloader:

    source_name: str

    def __init__(self):
        base = os.path.join(
            os.path.dirname(__file__), 'data', self.source_name)
        self.input_dir = os.path.join(base, 'repositories_metadata/')
        self.output_dir = os.path.join(base, 'raw_notebooks/')
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

    @abc.abstractmethod
    def download(self, metadata: dict, output_file: str) -> bool:
        """ Download notebooks

        :param metadata: repository metadata
        :param output_file: output filename (.ipynb)
        :return: True when the file is correctly downloaded or already existed
        """
        pass

    def bulk_download(self):
        for metadata_file in os.listdir(self.input_dir):
            metadata_file = os.path.join(self.input_dir, metadata_file)
            if not os.path.splitext(metadata_file)[1] == '.json':
                print('skipping', metadata_file)
                continue
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            output_file = os.path.join(
                self.output_dir, f"{metadata['id']}.ipynb")
            self.download(metadata, output_file)


class KaggleNotebookDownloader(_NotebookDownloader):

    source_name = 'Kaggle'

    def download(self, metadata, output_file):
        notebook_ref = metadata['name']

        if pd.isna(notebook_ref):
            print(f'[*NO REF] {notebook_ref}')
            return False

        download_path = os.path.dirname(output_file)
        # kaggle.api.kernels_pall downloads the nb to
        #  ('dl_notebook') and the metadata to
        # /path/kernel-metadata.json ('dl_metadata').
        files = {
            # notebook downloaded by Kaggle API (/path/<filename>.ipynb)
            'dl_notebook': os.path.join(
                download_path, os.path.basename(notebook_ref)
                ) + '.ipynb',
            # metadata downloaded by Kaggle API (/path/kernel-metadata.json)
            'dl_metadata': os.path.join(download_path, 'kernel-metadata.json'),
            # notebook destination (/path/<output_file>.ipynb)
            'notebook': output_file,
            # metadata destination (/path/<output_file>.ipynb.json)
            'metadata': f'{output_file}.json',
            }

        # Check if the file is already downloaded
        if os.path.isfile(files['notebook']):
            print(f'[!!EXIST] {notebook_ref}')
            return True
        try:
            kaggle.api.kernels_pull(notebook_ref, download_path, metadata=True)
            print(f'[Pulling] {notebook_ref}')

            if not os.path.isfile(files['dl_notebook']):
                print(f'[***FAIL] {notebook_ref}')
                # It is very important to delete the metadata file,
                # otherwise the following downloading will fail
                os.remove(files['dl_metadata'])
                return False

            # Rename the notebook file
            try:
                os.rename(files['dl_notebook'], files['notebook'])
            except FileNotFoundError as err:
                print("Exception: ", err)
                return False

            # Rename the metadata file
            try:
                os.rename(files['dl_metadata'], files['metadata'])
            except FileNotFoundError as err:
                print("Exception: ", err)
                return False
        except Exception as err:
            print("Exception: ", err)
            return False
        return True


class GithubNotebookDownloader(_NotebookDownloader):

    source_name = 'Github'

    def download(self, metadata, output_file):
        raise NotImplementedError


def main():

    s = KaggleNotebookDownloader()
    s.bulk_download()

    s = GithubNotebookDownloader()
    s.bulk_download()


if __name__ == '__main__':
    main()
