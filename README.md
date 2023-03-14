# Knowledge base indexer

## Setup

### Run locally

```shell
virtualenv venv
. venv/bin/activate
pip install -e .
python -m spacy download en_core_web_sm
python -m spacy download en_core_web_md
```


### Run with Docker

```shell
docker build . --file docker/Dockerfile --tag kb-indexer
docker run --name kb-indexer --env-file .env kb-indexer  # See env variables below
docker exec -it kb-indexer bash
```


## Configuration

The following environment variables should be defined:

```
DATA_DIR="/app/data"
ELASTICSEARCH_HOST="https://host:PORT/path/"
ELASTICSEARCH_USERNAME="<es username>"
ELASTICSEARCH_PASSWORD="<es password>"
KAGGLE_USERNAME="<A Kaggle username>"
KAGGLE_KEY="<A Kaggle API key>"
GITHUB_API_TOKEN="<A GitHub API token>"
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
