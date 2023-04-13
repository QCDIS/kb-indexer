# Field mappings
GITHUB_MATADATA_MAPPING = {
    'docid': ['docid'],
    "stargazers_count": [], 
    "forks_count": [], 
    'size': ['size'],
    'html_url': ['html_url'],
    'git_url': ['git_url'], 
    'source': [], 
    'code_file': ['name'], 
}

KAGGLE_METADATA_MAPPING = {
    'docid': ['docid'], 
    'source_id': ['id'],
    'name': ['title'],
    'file_name': ['code_file']
}

COMMON_CONTENT_MAPPING = {
    'description': ['md_text_clean'],
    'language': ['language'],
    'num_cells': ['num_cells'],
    'num_code_cells': ['num_code_cells'],
    'num_md_cells': ['num_md_cells'],
    'len_md_text': ['len_md_text']
}

GITHUB_CONTENT_MAPPING = {
    'docid': ['docid'],
    'name': ['title'],
    'description': ['description'],
    'language': ['language'],
    'num_cells': ['num_cells'],
    'num_code_cells': ['num_code_cells'],
    'num_md_cells': ['num_md_cells'],
    'len_md_text': ['len_md_text'],
    'summarization_t5': ['summarization_t5'],
    'summarization_relevance': ['summarization_relevance'],
    'summarization_confidence': ['summarization_confidence']
}

KAGGLE_CONTENT_MAPPING = {
    'docid': ['docid'], 
    'description': ['description'], 
    'language': ['language'],
    'num_cells': ['num_cells'],
    'num_code_cells': ['num_code_cells'],
    'num_md_cells': ['num_md_cells'],
    'len_md_text': ['len_md_text'],
    'summarization_t5': ['summarization_t5'],
    'summarization_relevance': ['summarization_relevance'],
    'summarization_confidence': ['summarization_confidence']
}
