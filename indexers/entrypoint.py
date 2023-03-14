#!/bin/env/python3
from threading import Event

import click

from .api.entrypoint import api
from .dataset.entrypoint import dataset
from .notebook.entrypoint import notebook
from .web.entrypoint import web


@click.group(
    help='Indexing pipeline for the ENVRI Knowledge Base',
    )
def cli():
    pass


@cli.command(help='Wait forever.')
def wait():
    click.echo('Waiting...')
    Event().wait()


cli.add_command(api)
cli.add_command(dataset)
cli.add_command(notebook)
cli.add_command(web)


if __name__ == '__main__':
    cli()
