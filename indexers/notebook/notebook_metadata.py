import json
from .metadata_mappings import GITHUB_MATADATA_MAPPING
from .metadata_mappings import KAGGLE_METADATA_MAPPING


class NotebookMetadata: 
    ''' Get metadata for each notebook 
    '''
    def __init__(self, source_name: str, notebook_metadata: dict):
        self.source_name = source_name
        self.notebook_metadata = notebook_metadata
        self.mapped_metadata = None

    def get_metadata(self):
        actions = {
            'Kaggle': self.get_kaggle_metadata,
            'Github': self.get_github_metadata
        }
        action_func = actions.get(self.source_name, self.get_default_metadata)
        action_func()
        return self.mapped_metadata

    def get_kaggle_metadata(self):
        mapped_metadata = self.map_metadata(self.notebook_metadata, KAGGLE_METADATA_MAPPING)
        # Add html and source
        mapped_metadata['html_url'] = "https://www.kaggle.com/code/" + mapped_metadata['source_id']
        mapped_metadata['source'] = 'Kaggle'
        self.mapped_metadata = mapped_metadata

    def get_github_metadata(self): 
        mapped_metadata = self.map_metadata(self.notebook_metadata, GITHUB_MATADATA_MAPPING)
        # Add source
        mapped_metadata['source'] = 'Github'
        self.mapped_metadata = mapped_metadata
    
    def get_default_metadata(self):
        pass
        # handle other input types

    @staticmethod
    def map_metadata(metadata: dict, metadata_mapping: dict): 
        mapped_metadata = {}
        for new_key, old_keys in metadata_mapping.items():
            mapped_value = None
            for old_key in old_keys:
                if old_key in metadata:
                    mapped_value = metadata[old_key]
                    break
            mapped_metadata[new_key] = mapped_value
        return mapped_metadata


if __name__ == '__main__': 
    metadata_file = './data/notebook/samples/NB_00a0e2f009613d9010772adffff1bce77dc9d6c332f006bc72cdffc2605c20ce.json'
    with open(metadata_file) as f: 
        notebook_metadata = json.load(f)
    metadata = NotebookMetadata('Github', notebook_metadata).get_metadata()
    print(metadata)