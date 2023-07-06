import os
import json
import click
import requests
from dotenv import load_dotenv
from termcolor import colored

# Load environment variables
load_dotenv()

GRAPHQL_URL = os.getenv("DAGSTER_GRAPHQL_URL")
API_TOKEN = os.getenv("DAGSTER_CLOUD_API_TOKEN")

headers = {
    "Content-Type": "application/json",
    "Dagster-Cloud-Api-Token": API_TOKEN,
}


@click.command()
@click.option('--asset', default=None, help='Get asset metrics')
def metrics(asset):
    """List your asset's metrics"""
    click.echo('\n')
    click.echo(colored(f"Asset: ", 'yellow') + colored(f"{asset}", 'green'))
    click.echo('\n')