import os 
import pandas as pd
import kaggle
import time
from datetime import timedelta
from memory_profiler import profile
 
class NotebookCrawler: 
    pass


class KaggleNotebookCrawler: 
    METADATA_FILE_NAME = "kernel-metadata.json"

    def __init__(self, df_queries, KERNEL_DOWNLOAD_PATH, KAGGLE_DOWNLOAD_LOG_FILE, KAGGLE_SEARCH_LOG_FILE):
        self.KERNEL_DOWNLOAD_PATH = KERNEL_DOWNLOAD_PATH
        self.KAGGLE_DOWNLOAD_LOG_FILE = KAGGLE_DOWNLOAD_LOG_FILE
        self.KAGGLE_SEARCH_LOG_FILE = KAGGLE_SEARCH_LOG_FILE
        self.df_queries = df_queries    

    def search_kernels(self, query, page_range): 
        ''' Search Kaggle kernels using given query
        '''
        kernels = []
        for page in range(1, page_range+1): 
            kernel_list = []
            try: 
                kernel_list = kaggle.api.kernels_list(search=query, page=page)
                print(f'Crawling page {page}')                
                if len(kernel_list) == 0: 
                    break
                else: 
                    kernels.extend(kernel_list)
            # Skip the pages that cause ApiException
            except kaggle.rest.ApiException as e:  
                # print(e)
                continue

        # Extract the `title` and the `ref` of returned Kaggle kernels
        results = []
        for kernel in kernels:
            results.append({
                'query': query, 
                'title': kernel.title, 
                'kernel_ref': kernel.ref})
        
        print('\n')
        return pd.DataFrame(results)

    def download_kernel(self, kernel_ref):
        ''' Download the kernels together with the metadata file

        Args: 
            - kernel_ref: the ID used by Kaggle to denote one notebook. 
        
        Return: 
            - Boolean: Only True when the file is correctly downloaded or already exists. 


        The notebook will be downloaded as 'dirname_basename' of `kernel_ref`. 

        For example, given kernel_ref = 'buddhiniw/breast-cancer-prediction', 
        there will be two files downloaded: 
            - buddhiniw_breast-cancer-prediction.ipynb
            - buddhiniw_breast-cancer-prediction.json
        '''
        download_path = self.KERNEL_DOWNLOAD_PATH
        file_name = os.path.dirname(kernel_ref) + '_' + os.path.basename(kernel_ref)

        # Check if the file is already downloaded
        if self.file_exists(file_name): 
            print(f'[!!EXIST] {kernel_ref}')
            return True    
        try: 
            kaggle.api.kernels_pull(kernel_ref, download_path, metadata=True)
            print(f'[Pulling] {kernel_ref}')
            
            old_file_name = os.path.basename(kernel_ref)
            if not self.file_exists(old_file_name): 
                print(f'[***FAIL] {kernel_ref}')
                old_metadata = os.path.join(download_path, self.METADATA_FILE_NAME)

                # It is very important to delete the metadata file, otherwise the following downloading will fail
                os.remove(old_metadata)
                return False

            # Rename the notebook file
            try: 
                old_file = os.path.join(download_path, os.path.basename(kernel_ref)) + '.ipynb'
                new_file = os.path.join(download_path, file_name) + '.ipynb'
                os.rename(old_file, new_file)
            except FileNotFoundError as err: 
                print("Exception: ", err)
                return False

            # Rename the metadata file
            try: 
                old_metadata = os.path.join(download_path, self.METADATA_FILE_NAME)
                new_metadata = os.path.join(download_path, file_name) + '.json'
                os.rename(old_metadata, new_metadata)
            except FileNotFoundError as err:
                print("Exception: ", err)
                return False
        except Exception as err: 
            print("Exception: ", err)
            return False
        return True

    def file_exists(self, file_name): 
        file_path = os.path.join(self.KERNEL_DOWNLOAD_PATH, file_name) + ".ipynb" 
        if os.path.exists(file_path): 
            return True
        else: 
            return False
        
    def has_results(kernel_list): 
        if len(kernel_list) == 0:
            return False
        else: 
            return True   
    
    # memory_profile decorator
    @profile
    def bulk_search(self, page_range): 
        '''' Search for notebooks '''
        df_queries = self.df_queries
        df_notebooks = pd.DataFrame()
        # Search notebooks for each query
        start = time.time()
        for i, query in enumerate(df_queries['queries']): 
            print(f'---------------- Query [{i+1}]: {query} ----------------')
            # To save the memory, we write the searching results to disk for every 10 queries 
            df_notebooks = pd.concat([df_notebooks, self.search_kernels(query, page_range)])
            df_notebooks.drop_duplicates(inplace=True)
            if (i+1)%10 == 0 or i+1 == len(df_queries): 
                # Update notebook search logs
                try: 
                    search_logs = pd.read_csv(self.KAGGLE_SEARCH_LOG_FILE)
                except Exception as e:
                    print(e) 
                    search_logs = pd.DataFrame(columns=df_notebooks.columns)

                if df_notebooks.empty: 
                    search_all = search_logs
                else: 
                    search_all = search_logs.merge(df_notebooks, how='outer')
                search_all.drop_duplicates(inplace=True)
                search_all.to_csv(self.KAGGLE_SEARCH_LOG_FILE, index=False)
                end = time.time()
                print(f'>>>>> Saving {len(df_notebooks)} searching results to disk...')
                print(f'>>>>> Time elapsed: {str(timedelta(seconds=int(end-start)))}\n\n')
                # Reset the notebooks after saving to the log file
                df_notebooks.drop(df_notebooks.index, inplace=True) 
        return search_all

    # memory_profile decorator
    @profile
    def bulk_download(self, df_notebooks): 
        ''' Download a bunch of notebooks specified inside `df_notebooks`'''
        # Read notebook download logs and filter out the new notbooks to download
        try: 
            download_logs = pd.read_csv(self.KAGGLE_DOWNLOAD_LOG_FILE)
        except Exception as e:
            print(e) 
            download_logs = pd.DataFrame(columns=df_notebooks.columns)
        
        merged = df_notebooks.merge(download_logs, how='left', indicator=True)
        new_notebooks = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
        new_notebooks.reset_index(inplace=True, drop=True)
        print(f'--------------------------- {len(new_notebooks)} new Notebooks --------------------------------')
        print(f'{new_notebooks}')
        print(f'----------------------------------------------------------------------------\n\n')

        # Download the notebooks and keep track of downloaded notebooks 
        start = time.time()
        downloaded_notebooks = pd.DataFrame()
        print(f'------------------ {0} - {49}  notebooks -------------------')
        for j in range(len(new_notebooks)): 
            # Download the notebooks
            kernel_ref = new_notebooks.iloc[j]['kernel_ref']
            if self.download_kernel(kernel_ref):
                downloaded_notebooks = pd.concat([downloaded_notebooks, new_notebooks.iloc[[j]]])

            if (j+1)%50==0 or j+1==len(new_notebooks): 
                # Update notebook download logs for every 100 notebooks
                try: 
                    download_logs = pd.read_csv(self.KAGGLE_DOWNLOAD_LOG_FILE)
                except Exception as e:
                    print(e) 
                    download_logs = pd.DataFrame(columns=downloaded_notebooks.columns)

                print(f'downloaded_notebooks.empty: {downloaded_notebooks.empty}')
                
                if downloaded_notebooks.empty: 
                    download_all = download_logs
                else: 
                    download_all = download_logs.merge(downloaded_notebooks, how='outer')
                download_all.drop_duplicates(inplace=True)
                # Save notebook names, IDs etc to .csv file. 
                download_all.to_csv(self.KAGGLE_DOWNLOAD_LOG_FILE, index=False)
                end = time.time()
                print(f'\n\n>>>>> Saving {len(downloaded_notebooks)} downloaded results to disk...')
                print(f'>>>>> Time elapsed: {str(timedelta(seconds=int(end-start)))}\n\n')
                # Reset downloaded_notebooks
                downloaded_notebooks.drop(downloaded_notebooks.index, inplace=True) 
                print(f'------------------ {j+1} - {j+50}  notebooks -------------------')

        return True


    def crawl_notebooks(self, page_range, re_search=False):
        ''' Search and download notebooks using given queries 

        The notebooks will be downloaed to disk
        '''
        if re_search==True: 
            df_notebooks = self.bulk_search(page_range)
        else: 
            try: 
                df_notebooks = pd.read_csv(self.KAGGLE_SEARCH_LOG_FILE)
            except Exception as e:
                print(f'[SearchLog ERROR(self-defined)] There is no search log file specified!') 
        
        self.bulk_download(df_notebooks)
        return True


def main():
    # Check if the current working path is `indexers``, if not terminate the
    # program
    if os.path.basename(os.getcwd()) != 'indexers':
        print(f'Please navigate to `indexers` directory and run: \n'
              f'`python -m notebooksearch.notebook_indexing`\n')
        return False

    KERNEL_DOWNLOAD_PATH = os.path.join(os.getcwd(), 'notebooksearch/Raw_notebooks/Kaggle')
    KAGGLE_DOWNLOAD_LOG_FILE = os.path.join(os.getcwd(), 'notebooksearch/Raw_notebooks/logs/kaggle_download_log.csv')
    KAGGLE_SEARCH_LOG_FILE = os.path.join(os.getcwd(), 'notebooksearch/Raw_notebooks/logs/kaggle_search_log.csv')
    QUERY_FILE = os.path.join(os.getcwd(), 'notebooksearch/Queries/kaggle_crawler_queries.csv')

    # Read queries
    df_queries = pd.read_csv(QUERY_FILE)
    # queries = ['wsi']
    # df_queries = pd.DataFrame(queries, columns= ['queries'])
    # print(df_queries)

    crawler = KaggleNotebookCrawler(df_queries, KERNEL_DOWNLOAD_PATH, KAGGLE_DOWNLOAD_LOG_FILE, KAGGLE_SEARCH_LOG_FILE)
    result = crawler.crawl_notebooks(page_range=1, re_search=True)
    return result

if __name__ == '__main__':
    main()

