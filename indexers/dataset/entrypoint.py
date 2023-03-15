import inspect
import types
from typing import Type

import click

from .pipeline import Pipeline
from .repositories.common import Repository
from . import repositories


class RepositoryMapper:
    def __init__(self):
        self._discover()

    def _discover(self):
        self.repos = {}
        for _, m in inspect.getmembers(repositories):
            if isinstance(m, types.ModuleType):
                for _, c in inspect.getmembers(m):
                    if (inspect.isclass(c)
                            and issubclass(c, Repository)
                            and (c is not Repository)):
                        self.repos[c.name] = c

    def resolve(self, repo_name) -> Type[Repository]:
        if repo_name not in self.repos:
            raise ValueError(f'Unknown repository: {repo_name}')
        return self.repos[repo_name]


def list_repos(ctx, _, value):
    if not value or ctx.resilient_parsing:
        return
    repo_mapper = RepositoryMapper()
    for name in repo_mapper.repos.keys():
        click.echo(name)
    ctx.exit()


@click.group(
    help='Dataset indexer.',
    invoke_without_command=True,
    chain=True,
    )
@click.option(
    '-r', '--repos',
    help='Repositories (default: select all).',
    multiple=True,
    )
@click.option(
    '--list-repos',
    help='List repositories and exit.',
    is_flag=True,
    callback=list_repos,
    expose_value=False,
    is_eager=True,
    )
@click.option(
    '--reindex',
    help='Reindex all resources.',
    is_flag=True,
    )
@click.option(
    '--keep-files',
    help='Keep records after conversion or indexing.',
    is_flag=True,
    )
@click.pass_context
def dataset(ctx, repos, reindex, keep_files):
    repo_mapper = RepositoryMapper()
    if not repos:
        repos = list(repo_mapper.repos.keys())
    ctx.obj = [
        Pipeline(
            repo_mapper.resolve(repo),
            reindex=reindex,
            keep_files=keep_files,
            )
        for repo in repos]


@dataset.command(help='Download raw records.')
@click.option(
    '--max-records',
    help='Maximum number of records to download.',
    type=int,
    )
@click.pass_obj
def download(pipelines, max_records):
    for p in pipelines:
        p.download(max_records=max_records)


@dataset.command(help='Convert raw records.')
@click.pass_obj
def convert(pipelines):
    for p in pipelines:
        p.convert()


@dataset.command(help='Index converted records.')
@click.pass_obj
def index(pipelines):
    for p in pipelines:
        p.index()


@dataset.command(help='Full indexing pipeline (download, convert, index).')
@click.pass_obj
def pipeline(pipelines):
    for p in pipelines:
        p.run()
