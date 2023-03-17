import click

from . import check_disk
from . import check_es


@click.group(
    help='Check system data.',
    )
def check():
    pass


@check.command(
    help='Check all.',
    )
def all():
    print('\nDisk contents:')
    check_disk.main()

    print('\nElasticsearch contents:')
    check_es.main()


@check.command(
    help='Check disk contents.',
    )
def disk():
    check_disk.main()


@check.command(
    help='Check elasticsearch contents.',
    )
def es():
    check_es.main()
