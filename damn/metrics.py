import json
import click
import requests
from termcolor import colored

from .utils import load_config 

@click.command()
@click.option('--asset', default=None, help='Get asset metrics')
@click.option('--profile', default='prod', help='Profile to use')
def metrics(asset, profile):
    """List your asset's metrics"""
    # Get connector configs
    dagster_config = load_config('dagster', profile)

    # Set headers
    headers = {
        "Content-Type": "application/json",
        "Dagster-Cloud-Api-Token": dagster_config['api_token'],
    }
    click.echo('\n')
    click.echo(colored(f"Asset: ", 'yellow') + colored(f"{asset}", 'green'))
    click.echo('\n')