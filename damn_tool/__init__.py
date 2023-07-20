import click

from .ls import ls
from .show import show
from .metrics import metrics

@click.group()
@click.pass_context
def cli(ctx):
    pass

cli.add_command(ls)
cli.add_command(show)
cli.add_command(metrics)