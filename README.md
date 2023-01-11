# Knowledge base indexer

## Dev usage

Starting elasticsearch

```shell
docker compose -f docker-compose_es.yml up
```

Running notebook indexing

```shell
cd indexers
python -m notebooksearch.notebook_indexing
```