#!/bin/env/python3

import click

from .api.entrypoint import api


@click.group(
    help='Indexing pipeline for the ENVRI Knowledge Base',
    )
def cli():
    pass


cli.add_command(api)


if __name__ == '__main__':
    cli()
