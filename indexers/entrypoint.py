#!/bin/env/python3

import click

from .api.entrypoint import api
from .dataset.entrypoint import dataset
from .web.entrypoint import web


@click.group(
    help='Indexing pipeline for the ENVRI Knowledge Base',
    )
def cli():
    pass


cli.add_command(api)
cli.add_command(dataset)
cli.add_command(web)


if __name__ == '__main__':
    cli()
