import click

from .pipeline import indexing_pipeline


@click.group(
    help='API indexer.',
    )
def api():
    pass


@api.command(
    help='Full indexing pipeline.',
    )
def pipeline():
    indexing_pipeline()
