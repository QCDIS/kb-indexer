import os
import json
import pandas as pd
from tqdm import tqdm

from .. import utils

def list2str(l):
    ''' Convert a list of list str to str using recursive function.  
    '''
    texts = ''
    # Check if the list is empty 
    if l == []:
        return ''
    elif isinstance(l[0], str): 
        text = ' '.join(l)
        return text  
    else: 
        for item in l: 
            texts += list2str(item)

    return texts


def list2str_no_space(l):
    ''' Convert a list of list str to str using recursive function.  
    '''
    texts = ''
        
    # Check if the list is empty 
    if l == []:
        return ''
    elif isinstance(l[0], str): 
        text = ''.join(l)
        return text
    else: 
        for item in l: 
            texts += list2str_no_space(item)

    return texts


# Extract text and code from MD cells
class NotebookContents:
    ''' The class NotebookContents provide a set of tools to extract different contents from the notebooks. 
        Methods: 
            - extract_text_from_md(self, nb)
            - extract_code(self, nb)
    '''
    
    def __init__(self):
        pass
    
    def extract_text_from_md(self, notebook_json):
        
        ''' Extract text from the MD cells of given notebook
        Args: 
            - nb : dict. {"id": str, "contents": str}
        Return: 
            - md_text: str. the text from MD cells. 
        ''' 
        text_md = []
        # The extraction of text relies on unified structure of .ipynb files. 
        for cell in notebook_json["cells"]: 
            if cell["cell_type"] == "markdown" and "source" in cell.keys():
                text_md.append(cell["source"])
                text_md.append("\n")
            else: 
                continue
        return list2str_no_space(text_md)
        
        
    def extract_code(self, notebook_json):
        
        ''' Extract code from the code cells of given notebook
        There are two tricks involved in extracting code: 
        1. No space is added between code cells
        2. Add an "\n" at the end of each code cell
    
        Args: 
            - nb : dict. {"id": str, "contents": str}
        Return: 
            - md_text: str. the text from MD cells. 
        ''' 
        code = []
        # The extraction of text relies on unified structure of .ipynb files. 
        # Eliminate empty code cells 
        for cell in notebook_json["cells"]: 
            if cell["cell_type"] == "code" and "source" in cell.keys():
                code.append(cell["source"])
                code.append("\n")
            else: 
                continue
        return list2str_no_space(code)


    def extract_contents(self, notebook_json):
     # Extract text from MD cells
        md_text = self.extract_text_from_md(notebook_json)
        result = {'md_text': md_text}
        return result


class NotebookStatistics:
    ''' The class NotebookStatistics provides the statistical data and methods of computing those statistics given a series of notebooks.
        Attributes:
            - language: list.
            - num_code_cells: list. number of code cells
            - num_md_cells: list. number of markdown cells
            - len_md_text: list. text length in markdown cells

        Methods:
            - cal_language(self, notebook)
            - cal_num_cells(self, notebook)
            - cal_statistics(self, notebooks)
    '''

    def __init__(self):
        self.language = []
        self.num_code_cells = []
        self.num_md_cells = []
        self.len_md_text = []

    def cal_language(self, notebook_json):
        ''' Extract the language information of the given notebook
            Args:
                - notebook : dict. {"id": str, "contents": str}
            Return:
                - language: str. 'others' if there is no information about language
        '''
        if "metadata" not in notebook_json.keys():
            lang = 'others'

        elif 'language_info' not in notebook_json["metadata"].keys():
            lang = 'others'

        else:
            lang = notebook_json['metadata']['language_info']['name']
        return lang

    def cal_num_cells(self, notebook_json):
        ''' Calculate the number of code cells of the given notebook
            Args:
                - notebook : dict. {"id": str, "contents": str}
            Return:
                - num_code_cells: int. number of code cells
                - num_md_cells: int. number of MD cells
                - len_md_text: int. the number of lines of text in markdown cells of the given notebook
        '''
        num_code_cells = 0
        num_md_cells = 0
        len_md_text = 0

        # Sanity check 1: check if notebook has cells
        if "cells" in notebook_json.keys():
            for cell in notebook_json["cells"]:
                # Sanity check 2: check if cell_type is specified
                if "cell_type" not in cell.keys():
                    continue
                elif cell["cell_type"] == "code":
                    num_code_cells += 1
                elif cell["cell_type"] == "markdown" and "source" in cell.keys():
                    num_md_cells += 1
                    len_md_text += len(cell["source"])

        return num_code_cells, num_md_cells, len_md_text

    def cal_statistics(self, notebook_json):
        ''' Calculate all the statistics of the given notebooks
            Args:
                - notebooks : list of dict. Each element is a notebook
        '''
        language = self.cal_language(notebook_json)
        num_code_cells, num_md_cells, len_md_text = self.cal_num_cells(notebook_json)
        result = {
            'language': language,
            'num_cells': num_code_cells+num_md_cells,
            'num_code_cells': num_code_cells,
            'num_md_cells': num_md_cells,
            'len_md_text': len_md_text,
        }
        return result


class RawNotebookPreprocessor:
    ''' A class for handling raw notebooks. 
    
    Args: 
        - input_path: The path of a folder under which the .ipynb files reside. 
        - output_path: The path of a folder where the .csv files will be placed. 
    '''
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.directories = {
            'input_notebooks': f'data/{self.source_name}/raw_notebooks/',
            'input_metadata': f'data/{self.source_name}/notebooks_metadata/',
            'output': f'data/{self.source_name}/notebook_lists/',
            }
        self._add_abs_path_to_directories()
        self._make_directories()
        self.files = {
            'raw_notebooks': os.path.join(self.directories['output'],
                                          'raw_notebooks.csv'),
            'processed_notebooks': os.path.join(self.directories['output'],
                                                'processed_notebooks.csv'),
            'updated_processed_notebooks': os.path.join(
                self.directories['output'], 'updated_processed_notebooks.csv'),
            }

    def _add_abs_path_to_directories(self):
        for k, v in self.directories.items():
            self.directories[k] = os.path.join(os.path.dirname(__file__), v)

    def _make_directories(self):
        for d in self.directories.values():
            os.makedirs(d, exist_ok=True)

    def dump_raw_notebooks(self):
        ''' Dump raw notebooks to a .csv file. 

        And keep a record of notebook metadata in another .csv file
        '''
        contents = NotebookContents()
        raw_notebooks = []
        # Go through all the .ipynb file and store the contents in one single .csv file. 
        for path, _, files in os.walk(self.directories['input_notebooks']):
            for name in files:
                if name.endswith('.ipynb'): 
                    file_path = os.path.join(
                        self.directories['input_notebooks'], name)
                    notebook_json = utils.read_json_file(file_path)
                    notebook = json.dumps(notebook_json)

                    # Read metadata
                    metadata_path = os.path.join(
                        self.directories['input_metadata'], name[:-6]+'.json')
                    metadata = utils.read_json_file(metadata_path)

                    # Get HTML URL
                    html_url = self.get_html_url(source_id=metadata['id'])

                    # Extract md_text from the notebook contents
                    try: 
                        extracted_contents = contents.extract_contents(notebook_json)
                    except Exception as e: 
                        print(e)
                        continue

                    new_record = {
                        "source_id": metadata['id'], 
                        "name": metadata['title'], 
                        "file_name": name, 
                        "html_url": html_url, 
                        "description": extracted_contents['md_text'], 
                        "source": self.source_name,
                        "notebook_source_file": notebook
                        }
                    raw_notebooks.append(new_record)
                else: 
                    continue
        
        # Assign `docid` to each notebook
        df_raw_notebooks = pd.DataFrame.from_dict(raw_notebooks)
        df_raw_notebooks['docid'] = range(len(df_raw_notebooks))
        df_raw_notebooks['docid'] = df_raw_notebooks['docid'].apply(lambda x: self.source_name + str(x))
        print(f'Number of raw notebooks: {len(df_raw_notebooks)}\n')

        df_metadata = df_raw_notebooks.drop(columns=['notebook_source_file'])
        df_raw_notebooks = df_raw_notebooks[['docid', 'source_id', 'name', 'file_name', 'source', 'notebook_source_file']]

        # Save the resulting files
        notebook_file = self.files['raw_notebooks']
        metadata_file = self.files['processed_notebooks']

        print(f'Saving raw notebooks to: {notebook_file}\n')
        df_raw_notebooks.to_csv(notebook_file, index=False)

        print(f'Saving notebook metadata to: {metadata_file}\n')
        df_metadata.to_csv(metadata_file, index=False)


    def get_html_url(self, source_id):
        if self.source_name =='Kaggle':
            return ("https://www.kaggle.com/code/" + source_id)
        elif self.source_name =='Github':
            return ' '  # TODO
        else:
            raise ValueError

    def add_new_features(self):
        ''' Add new features extracted from notebook contents to existent records.

        Args:
            - df_features: pandas.DataFrame.
        '''
        df_features = self.extract_new_features(self.source_name)
        # df_notebooks = pd.read_csv(os.path.join(output_dir, source_name + '_raw_notebooks.csv'))
        df_metadata = pd.read_csv(self.files['processed_notebooks'])

        # Add features to preprocessed notebooks
        df_metadata = df_metadata.merge(df_features, how='left', on='docid')

        # Save updated metadata file
        metadata_file = self.files['updated_processed_notebooks']

        print(f'Saving updated notebook metadata to: {metadata_file}\n')
        df_metadata.to_csv(metadata_file, index=False)
        return True

    def extract_new_features(self, source_name):
        ''' Extract new features ['language', 'num_code_cells', 'num_md_cells', 'len_md_text']
        '''
        df_notebooks = pd.read_csv(self.files['raw_notebooks'])
        # Compute the statistics of notebooks
        statistics = NotebookStatistics()
        def extract_new_contents(notebook_str):
            notebook_json = json.loads(notebook_str)
            return statistics.cal_statistics(notebook_json)
        features = list(df_notebooks['notebook_source_file'].apply(extract_new_contents))

        df_features = pd.DataFrame.from_dict(features)
        df_features['docid'] = df_notebooks['docid']
        return df_features


def main():
    preprocessor = RawNotebookPreprocessor('Kaggle')
    preprocessor.dump_raw_notebooks()
    preprocessor.add_new_features()


if __name__ == '__main__': 
    main()
