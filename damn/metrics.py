import json
import click
import requests
from termcolor import colored

from .utils import load_config 

# Get connector configs
dagster_config = load_config('dagster', 'prod')

# Set headers
headers = {
    "Content-Type": "application/json",
    "Dagster-Cloud-Api-Token": dagster_config['api_token'],
}


@click.command()
@click.option('--asset', default=None, help='Get asset metrics')
def metrics(asset):
    """List your asset's metrics"""
    click.echo('\n')
    click.echo(colored(f"Asset: ", 'yellow') + colored(f"{asset}", 'green'))
    click.echo('\n')