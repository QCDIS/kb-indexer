import os
import json
import glob

from .. import utils
from .notebook_statistics import NotebookStatistics
from .notebook_contents import NotebookContents


class RawNotebookPreprocessor:
    def __init__(self, source_name: str):
        self.source_name = source_name

        data_dir = os.path.join(utils.get_data_dir(), 'notebook')
        self.directories = {
            'input': f'{data_dir}/{self.source_name}/raw_notebooks/',
            'output_summary': f'{data_dir}/'
                              f'{self.source_name}/notebooks_summaries/',
            'output_contents': f'{data_dir}/'
                              f'{self.source_name}/notebooks_contents/',
            }
        for d in self.directories.values():
            os.makedirs(d, exist_ok=True)

    def bulk_preprocess(self):

        filename_pattern = os.path.join(self.directories['input'], '*.ipynb')
        for filename in glob.glob(filename_pattern):
            with open(filename) as f:
                notebook = json.load(f)
            with open(filename + '.json') as f:
                notebook_metadata = json.load(f)

            basename = os.path.basename(filename)
            basename = os.path.splitext(basename)[0] + '.json'

            self.preprocess(
                notebook,
                notebook_metadata,
                os.path.join(self.directories['output_summary'], basename),
                os.path.join(self.directories['output_contents'], basename),
                )

    @staticmethod
    def preprocess(notebook: dict, notebook_metadata: dict,
                   summary_filename: str, contents_filename: str):
        """

        :param notebook: Notebook contents (parsed .ipynb)
        :param notebook_metadata: Notebook metadata
        :param summary_filename: Path to the output notebook summary (json)
        :param contents_filename: Path to the output notebook contents (json)
        :return:
        """

        try:
            extracted_contents = NotebookContents(notebook).get_contents()
        except Exception as e:
            print(e)
            return

        try:
            statistics = NotebookStatistics(notebook).get_statistics()
        except Exception as e:
            print(e)
            return

        summary_doc = {
            "id": notebook_metadata['id'],
            "name": notebook_metadata['description'],
            # FIXME: this is the title of Kaggle notebooks, but the description
            #  of GitHub repositories, so many notebooks will have the same
            #  name.
            "file_name": notebook_metadata['code_file'],
            "html_url": notebook_metadata['html_url'],
            "source": notebook_metadata['source'],
            "description": extracted_contents['md_text'],
            **statistics,
            }
        contents_doc = summary_doc.copy()
        contents_doc["notebook_source_file"] = json.dumps(notebook)

        with open(summary_filename, 'w') as f:
            json.dump(summary_doc, f)

        with open(contents_filename, 'w') as f:
            json.dump(contents_doc, f)