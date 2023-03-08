# Knowledge base indexer

## Setup (development)

```shell
virtualenv venv
. venv/bin/activate
pip install -e .
python -m spacy download en_core_web_sm
python -m spacy download en_core_web_md
```


## Configuration

The following environment variables should be defined:

```
ELASTICSEARCH_HOST
ELASTICSEARCH_USERNAME
ELASTICSEARCH_PASSWORD
KAGGLE_USERNAME
KAGGLE_KEY
GITHUB_API_TOKEN
```


## Usage

Several indexers (API, dataset, web, etc.) are available as subcommands of 
the `kb_indexer` command. To see them: 

```shell
kb_indexer --help
```

For indexer-specific help:

```shell
kb_indexer <indexer> --help
```

Examples:

```shell
kb_indexer api pipeline  # Full API indexing pipeline
kb_indexer web pipeline  # Full web indexing pipeline
kb_indexer notebook -r Kaggle search  # Search notebooks on Kaggle
kb_indexer notebook -r Kaggle index  # Index notebooks found on Kaggle
kb_indexer notebook -r GitHub pipeline  # Full pipeline (search+index) for notebooks from GitHub
kb_indexer dataset -r ICOS pipeline  # Full indexing pipeline for ICOS datasets
kb_indexer dataset pipeline  # Full indexing pipeline for ALL dataset sources
```
