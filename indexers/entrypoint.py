#!/bin/env/python3

import click


@click.group(
    help='Indexing pipeline for the ENVRI Knowledge Base',
    )
def cli():
    pass


if __name__ == '__main__':
    cli()
