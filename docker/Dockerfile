FROM python:3.10-slim-buster

WORKDIR /app
ENV DATA_DIR="/app/data"

# Required because kaggle complains on import
ENV KAGGLE_USERNAME="<A Kaggle username>"
ENV KAGGLE_KEY="<A Kaggle API key>"

COPY pyproject.toml requirements.txt ./
COPY indexers/ indexers/

RUN apt update && apt install -y libenchant-dev
RUN pip3 install . && \
    python3 -m spacy download en_core_web_sm && \
    python3 -m spacy download en_core_web_md

CMD ["kb_indexer", "wait"]