from typing import Type

import click

from .pipeline import Pipeline
from . import repositories


class RepositoryMapper:
    repos = {
        'Kaggle': repositories.KaggleRepository,
        'GitHub': repositories.GitHubRepository,
        }

    def __init__(self):
        pass

    def resolve(self, repo_name) -> Type[repositories.Repository]:
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
    help='Notebook indexer.',
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
@click.pass_context
def notebook(ctx, repos):
    repo_mapper = RepositoryMapper()
    if not repos:
        repos = list(repo_mapper.repos.keys())
    ctx.obj = [
        Pipeline(
            repo_mapper.resolve(repo),
            )
        for repo in repos]


@notebook.command(help='Search notebooks.')
@click.pass_obj
def search(pipelines):
    for p in pipelines:
        p.search()


# @notebook.command(help='Download notebooks.')
# @click.pass_obj
# def download(pipelines):
#     for p in pipelines:
#         p.download()
#
#
# @notebook.command(help='Preprocess notebooks.')
# @click.pass_obj
# def preprocess(pipelines):
#     for p in pipelines:
#         p.preprocess()
#

@notebook.command(help='Index notebooks.')
@click.pass_obj
def index(pipelines):
    for p in pipelines:
        p.index()


@notebook.command(help=('Full indexing pipeline '
                        '(search, index).'))
                      # '(search, download, preprocess, index).'))
@click.pass_obj
def pipeline(pipelines):
    for p in pipelines:
        p.run()
