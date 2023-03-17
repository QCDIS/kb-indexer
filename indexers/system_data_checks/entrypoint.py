import click

from . import check_es


@click.group(
    help='Check system data.',
    )
def check():
    pass


@check.command(
    help='Check elasticsearch contents.',
    )
def es():
    check_es.main()
