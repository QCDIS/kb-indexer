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

## Indexes

### notebooks

| Entry                | Example                                                                | `github_notebooks` | `kaggle_notebooks` | `kaggle_raw_notebooks` |
|----------------------|------------------------------------------------------------------------|--------------------|--------------------|------------------------|
| name                 | 'Introduction to GANs with Keras'                                      | x                  | x                  | x                      |
| full_name            |                                                                        | x                  |                    |                        |
| stargazers_count     |                                                                        | x                  |                    |                        |
| forks_count          |                                                                        | x                  |                    |                        |
| description          | (rendered notebook)                                                    | x                  | x                  |                        |
| size                 |                                                                        | x                  |                    |                        |
| language             | 'python'                                                               | x                  | x                  |                        |
| html_url             | 'https://www.kaggle.com/code/yushg123/introduction-to-gans-with-keras' | x                  | x                  |                        |
| git_url              |                                                                        | x                  |                    |                        |
| id                   |                                                                        | x                  |                    |                        |
| source               | 'Kaggle'                                                               | x                  | x                  | x                      |
| source_id            | 'yushg123/introduction-to-gans-with-keras'                             |                    | x                  | x                      |
| file_name            | 'yushg123_introduction-to-gans-with-keras.ipynb'                       |                    | x                  | x                      |
| docid                | 'Kaggle111'                                                            |                    | x                  | x                      |
| notebook_source_file | (json)                                                                 |                    | x                  | x                      |
| num_cells            | 42                                                                     |                    | x                  |                        |
| num_code_cells       | 18                                                                     |                    | x                  |                        |
| num_md_cells         | 24                                                                     |                    | x                  |                        |
| len_md_text          | 9362                                                                   |                    | x                  |                        |


### websearch

- `webcontents`: description, identifier, keywords, language,
  accessibilitySummary, contact, version, temporalCoverage, publisher, spatial,
  license, citation, genre, creator, modificationDate, distribution, image,
  thumbnailUrl, headline, abstract, theme, dateCreated, creditText,
  datePublished, producer, author, spatialCoverage, url, sponsor, size, sameAs,
  publication, provider, position, name, measurementTechnique, material,
  maintainer, locationCreated, issn, isPartOf, isBasedOn, isAccessibleForFree,
  includedInDataCatalog, editor, editEIDR, copyrightYear, copyrightNotice,
  copyrightHolder, contributor, contentReferenceTime, contentLocation,
  character, acquireLicensePage, accessModeSufficient, about, rights, relation,
  qualifiedRelation, qualifiedAttribution, previousVersion, nextVersion,
  landingPage, isReferencedBy, hasVersion, hasPolicy, hasCurrentVersion,
  useConstraints, status, spatialRepresentationType, spatialRepresentationInfo,
  scope, purpose, otherLocale, metadataProfile, metadataLinkage,
  metadataIdentifier, environmentDescription, distributionInfo,
  dataQualityInfo, contentInfo, ResearchInfrastructure, EssentialVariables,
  potentialTopics


### datasets

- `envri`: description, identifier, keywords, language, accessibilitySummary,
  contact, version, temporalCoverage, publisher, spatial, license, citation,
  genre, creator, modificationDate, distribution, image, thumbnailUrl,
  headline, abstract, theme, dateCreated, creditText, datePublished, producer,
  author, spatialCoverage, url, sponsor, size, sameAs, publication, provider,
  position, name, measurementTechnique, material, maintainer, locationCreated,
  issn, isPartOf, isBasedOn, isAccessibleForFree, includedInDataCatalog,
  editor, editEIDR, copyrightYear, copyrightNotice, copyrightHolder,
  contributor, contentReferenceTime, contentLocation, character,
  acquireLicensePage, accessModeSufficient, about, rights, relation,
  qualifiedRelation, qualifiedAttribution, previousVersion, nextVersion,
  landingPage, isReferencedBy, hasVersion, hasPolicy, hasCurrentVersion,
  useConstraints, status, spatialRepresentationType, spatialRepresentationInfo,
  scope, purpose, otherLocale, metadataProfile, metadataLinkage,
  metadataIdentifier, environmentDescription, distributionInfo,
  dataQualityInfo, contentInfo, ResearchInfrastructure, EssentialVariables,
  potentialTopics
