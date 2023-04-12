import tempfile
import os
import ast
from comment_parser import comment_parser
import re
import json
from bs4 import BeautifulSoup

# what is meaning
class NotebookContents:
    ''' The class NotebookContents provide a set of tools to extract different contents from the notebooks. 
        Attributes: md_text,md_titles,code_comments,code,md_text_clean
        Methods: 
            - extract_text_from_md()
            - extract_code()
    '''
    
    def __init__(self, notebook):
        self.notebook = notebook
        self.md_text = ''
        self.md_text_clean = ''
        self.code = ''
        self.code_comments = ''

    @staticmethod
    def _list2str_no_space(l): 
        ''' Convert a list of list str to str using recursive function. 
        The depth of lists are unknown. 
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
                texts += NotebookContents._list2str_no_space(item)
        return texts
    
    def extract_text_from_md(self):
        
        ''' Extract text from the MD cells of given notebook
        Args: 
            - nb : dict. {"id": str, "contents": str}
        Return: 
            - md_text: str. the text from MD cells. 
        ''' 
        notebook = self.notebook
        text_md = []
        # The extraction of text relies on unified structure of .ipynb files. 
        for cell in notebook["cells"]: 
            if cell["cell_type"] == "markdown" and "source" in cell.keys():
                text_md.append(cell["source"])
                text_md.append("\n")
            else: 
                continue
        self.md_text = self._list2str_no_space(text_md)
        
    
    def clean_md_text(self):
        ''' Clean the md text. 
        Remove htmls, urls and stopwords
            
        '''
        def remove_html(text):
            text = BeautifulSoup(text, "lxml").get_text()
            return text.replace(u'\xa0', u'')
        
        def extract_urls(text):
            pattern = r'(http|ftp|https):\/\/([\w\-_]+(?:(?:\.[\w\-_]+)+))([\w\-\.,@?^=%&:/~\+#]*[\w\-\@?^=%&/~\+#])?'    
            url_tuples = re.findall(pattern, text)
            urls = []
            for item in url_tuples: 
                url = item[0] + '://' + item[1] + item[2]
                urls.append(url)
            return urls
        
        def remove_urls(text):
            ''' Remove urls
            '''
            urls = extract_urls(text)
            for url in urls: 
                text = text.replace(url, '')
            return text

        def remove_blank_lines(text): 
            return re.sub(r'(\n)+\s*|(\n)+', '\n', text)

        text = self.md_text  
        text = remove_html(text)
        text = remove_urls(text)
        text = remove_blank_lines(text)
        
        self.md_text_clean = text


    def extract_code(self):
        
        ''' Extract code from the code cells of given notebook
        There are two tricks involved in extracting code: 
        1. No space is added between code cells
        2. Add an "\n" at the end of each code cell
    
        Args: 
            - nb : dict. {"id": str, "contents": str}
        Return: 
            - md_text: str. the text from MD cells. 
        ''' 
        notebook = self.notebook
        code = []
        # The extraction of text relies on unified structure of .ipynb files. 
        # Eliminate empty code cells 
        for cell in notebook["cells"]: 
            if cell["cell_type"] == "code" and "source" in cell.keys():
                code.append(cell["source"])
                code.append("\n")
            else: 
                continue
        self.code = self._list2str_no_space(code)
    

    def extract_code_comment(self): 
        ''' Extract code comments using the library `comment_parser`. 
        Excluding commented codes. 
        '''
        def is_valid_code(text):
            try: 
                ast.parse(text)
            except SyntaxError: 
                return False
            return True
        
        def write_code_to_file(code_string, filename):
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.py', dir='.') as temp:
                temp.write(code_string)
                temp_path = temp.name
            os.rename(temp_path, filename)
            return filename
        
        code_string = self.code
        filename = 'code_temp.py'
        temp_path = write_code_to_file(code_string, filename)

        text_descriptions = []
        try: 
            comments = comment_parser.extract_comments(temp_path)
        except Exception as e: 
            comments = []
        for comment in comments:
            text = comment.text()
            if not is_valid_code(text): 
                text_descriptions.append(text)
        os.unlink(temp_path)
        self.code_comments = '\n'.join(text_descriptions)


    def extract_contents(self):
     # The pipepline for extracting contents from notebooks
        self.extract_text_from_md()
        self.clean_md_text()
        self.extract_code()
        self.extract_code_comment()
    
    def get_contents(self): 
        self.extract_contents()
        result = {
            'md_text': self.md_text,
            'md_text_clean': self.md_text_clean,
            'code': self.code,
            'code_comments': self.code_comments
            }
        return result
    
if __name__ == '__main__': 
    filename = '../data/notebook/samples/NB_7e2ff2b105f57d2525252ffdc7c679561baee79561e6490a7de00593f674d4e7.ipynb'
    with open(filename) as f:
        notebook = json.load(f)
    notebook_contents = NotebookContents(notebook)
    result = notebook_contents.get_contents()
    # print(result)
    with open('test_code.py', 'w') as f: 
        f.write(result['code'])
    with open('test_md_clean.md', 'w') as f: 
        f.write(result['md_text_clean'])
    # with open ('test.json', 'w') as f:
    #     json.dump(result, f, ensure_ascii=False, indent=None)
