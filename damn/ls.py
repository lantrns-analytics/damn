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
@click.option('--prefix', default=None, help='Get list of assets with a given prefix')
def ls(prefix):
    """List your platform's data assets"""
    if prefix:
      prefix_list = prefix.split('/')
      query = f"""
      query AssetsQuery {{
        assetsOrError(prefix: {json.dumps(prefix_list)}) {{
          ... on AssetConnection {{
            nodes {{
              key {{
                path
              }}
            }}
          }}
        }}
      }}
      """
    else:
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
        GRAPHQL_URL, # type: ignore
        headers=headers, # type: ignore
        json={"query": query}
    )
    
    response.raise_for_status()
    
    data = response.json()

    # Extract asset keys and print them
    click.echo('\n')
    for node in data['data']['assetsOrError']['nodes']:
        asset_key = "/".join(node['key']['path'])
        click.echo(colored(f'- {asset_key}', 'cyan'))
    click.echo('\n')