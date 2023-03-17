import os
import json
import glob

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
            extracted_contents = NotebookContents().extract_contents(notebook)
        except Exception as e:
            print(e)
            return

        try:
            statistics = NotebookStatistics().cal_statistics(notebook)
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
        contents_doc["notebook_source_file"] = notebook

        with open(summary_filename, 'w') as f:
            json.dump(summary_doc, f)

        with open(contents_filename, 'w') as f:
            json.dump(contents_doc, f)
