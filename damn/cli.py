import os
import json
import click
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

GRAPHQL_URL = "https://discursus.dagster.cloud/prod/graphql"
API_TOKEN = os.getenv("DAGSTER_CLOUD_API_TOKEN")

headers = {
    "Content-Type": "application/json",
    "Dagster-Cloud-Api-Token": API_TOKEN,
}

@click.group()
def cli():
    pass

@cli.command()
def ls():
    """List your platform's data assets"""
    query = """
    query AssetsQuery {
      assetsOrError {
        ... on AssetConnection {
          nodes {
            key {
              path
            }
          }
        }
      }
    }
    """

    response = requests.post(
        GRAPHQL_URL,
        headers=headers,
        json={"query": query}
    )
    
    response.raise_for_status()
    
    data = response.json()

    # Extract asset keys and print them
    for node in data['data']['assetsOrError']['nodes']:
        asset_key = "/".join(node['key']['path'])
        click.echo(f'- {asset_key}')

if __name__ == '__main__':
    cli()