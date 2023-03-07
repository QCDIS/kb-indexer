import click

from .pipeline import index_all_research_infrastructures


@click.group(
    help='Web indexer.',
    )
def web():
    pass


@web.command(
    help='Full indexing pipeline.',
    )
def pipeline():
    index_all_research_infrastructures()
