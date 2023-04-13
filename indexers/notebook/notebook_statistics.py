import json

class NotebookStatistics: 
    ''' The class NotebookStatistics provides the statistical data and methods of computing those statistics given a series of notebooks. 
        Attributes: 
            - notebook: dict, the raw content of notebook loaded by json 
            - language: list. 
            - num_code_cells: list. number of code cells
            - num_md_cells: list. number of markdown cells
            - len_md_text: list. text length in markdown cells
            
        Methods: 
            - cal_language(self, notebook)
            - cal_num_cells(self, notebook)
            - cal_statistics(self, notebooks)
    '''
    
    def __init__(self, notebook): 
        self.notebook = notebook
        self.language = None
        self.num_code_cells = None
        self.num_md_cells = None
        self.len_md_text = None
    
    def cal_language(self):
        ''' Extract the language information of the given notebook
            Return: 
                - language: str. 'others' if there is no information about language
        '''
        notebook = self.notebook
        if "metadata" not in notebook.keys(): 
            lang = 'others'
            
        elif 'language_info' not in notebook["metadata"].keys(): 
            lang = 'others'
            
        else: 
            lang = notebook['metadata']['language_info']['name']
        self.language = lang
    
    def cal_num_cells(self): 
        ''' Calculate the number of code cells of the given notebook
            Return: 
                - num_code_cells: int. number of code cells
                - num_md_cells: int. number of MD cells
                - len_md_text: int. the number of lines of text in markdown cells of the given notebook
        '''
        notebook = self.notebook        
        num_code_cells = 0
        num_md_cells = 0
        len_md_text = 0
        
        # Sanity check 1: check if notebook has cells
        if "cells" in notebook.keys(): 
            for cell in notebook["cells"]: 
                # Sanity check 2: check if cell_type is specified
                if "cell_type" not in cell.keys(): 
                    continue
                elif cell["cell_type"] == "code": 
                    num_code_cells += 1
                elif cell["cell_type"] == "markdown" and "source" in cell.keys(): 
                    num_md_cells += 1 
                    len_md_text += len(cell["source"])
            
        self.num_code_cells = num_code_cells
        self.num_md_cells = num_md_cells
        self.len_md_text = len_md_text
    
    def cal_statisitcs(self): 
        ''' Pipeline for calculating all the statistics
        '''
        self.cal_language()
        self.cal_num_cells() 


    def get_statistics(self):
        ''' Calculate all the statistics of the given notebooks
            Args: 
                - notebooks : list of dict. Each element is a notebook
        '''
        self.cal_statisitcs()
        result = {
            'language': self.language, 
            'num_cells': self.num_code_cells + self.num_md_cells, 
            'num_code_cells': self.num_code_cells, 
            'num_md_cells': self.num_md_cells, 
            'len_md_text': self.len_md_text, 
        }
        return result

if __name__ == '__main__': 
    filename = '../data/notebook/samples/NB_7e2ff2b105f57d2525252ffdc7c679561baee79561e6490a7de00593f674d4e7.ipynb'
    with open(filename) as f:
        notebook = json.load(f)
    notebook_statistics = NotebookStatistics(notebook)
    print(notebook_statistics.get_statistics())