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
    click.echo(json.dumps(data, indent=2))

if __name__ == '__main__':
    cli()