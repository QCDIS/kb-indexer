# Knowledge base indexer

## Setup

Install language processing data:

```shell
python -m spacy download en 
python -m spacy download en_core_web_md
python -m nltk.downloader omw-1.4
```


## Usage

Starting elasticsearch (dev):

```shell
docker compose -f docker-compose_es.yml up
```

Running the indexing pipelines:

```shell
cd indexers
python -m api.pipeline
python -m dataset.pipeline
python -m notebook.pipeline
python -m web.pipeline
```

## Indexes

### notebooks

| Entry                | Example                                                                | `github_notebooks` | `kaggle_notebooks` | `kaggle_raw_notebooks` |
|----------------------|------------------------------------------------------------------------|--------------------|--------------------|------------------------|
| (sr) name            | introduction-to-gans / Introduction to GANs with Keras                 | x                  | x                  | x                      |
| full_name            | 'yushg123/introduction-to-gans-with-keras'                             | x                  |                    |                        |
| (r) stargazers_count | 3                                                                      | x                  |                    |                        |
| forks_count          | 1                                                                      | x                  |                    |                        |
| (sr) description     | (short gh description) / (rendered notebook)                           | x                  | x                  |                        |
| (r)size              | 524                                                                    | x                  |                    |                        |
| language             | 'Jupyter Notebook' / 'python'                                          | x                  | x                  |                        |
| (r) html_url         | 'https://www.kaggle.com/code/yushg123/introduction-to-gans-with-keras' | x                  | x                  |                        |
| (r) git_url          | 'https://github.com/dtrupenn/Tetris.git'                               | x                  |                    |                        |
| id                   | 1829838                                                                | x                  |                    |                        |
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
