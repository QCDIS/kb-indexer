# Knowledge base indexer

## Dev usage

Starting elasticsearch

```shell
docker compose -f docker-compose_es.yml up
```

Running notebook indexing

```shell
cd indexers
python -m notebooksearch.notebook_crawling  # saves notebooks to Raw_notebooks/
python -m notebooksearch.notebook_preprocessing  # creates CSVs in Notebooks/
python -m notebooksearch.notebook_indexing
```

Install language pipeline for web and dataset indexers

```shell
python -m spacy download en 
python -m spacy download en_core_web_md
python -m nltk.downloader omw-1.4
```

Run web indexer

```shell
cd indexers
python -m websearch.adhoc_crawler
```

Run dataset indexer

```shell
cd indexers
python -m datasetsearch.adhoc_DatasetRecords
```