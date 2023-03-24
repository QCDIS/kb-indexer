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
@click.option(
    '--reindex',
    help='Reindex all resources.',
    is_flag=True,
    )
def pipeline(reindex):
    index_all_research_infrastructures(reindex=reindex)
