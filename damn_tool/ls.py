import click
import json
import pyperclip
import requests
from termcolor import colored

from .utils.helpers import load_config, run_and_capture


def get_dagster_assets(prefix, profile):
    # Get connector configs
    dagster_config = load_config('dagster', profile)

    # Set headers
    headers = {
        "Content-Type": "application/json",
        "Dagster-Cloud-Api-Token": dagster_config['api_token'],
    }

    # Get data
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
        dagster_config['endpoint'], # type: ignore
        headers=headers, # type: ignore
        json={"query": query}
    )
    
    response.raise_for_status()
    
    return response.json()


def display_assets(data):
    # Extract asset keys and print them
    for node in data['data']['assetsOrError']['nodes']:
        asset_key = "/".join(node['key']['path'])
        click.echo(colored(f'- {asset_key}', 'cyan'))


@click.command()
@click.option('--prefix', default=None, help='Get list of assets with a given prefix')
@click.option('--profile', default='prod', help='Profile to use')
@click.option('--copy-output', is_flag=True, help='Copy command output to clipboard')
def ls(prefix, profile, copy_output):
    """List your platform's data assets"""
    data = get_dagster_assets(prefix, profile)

    if copy_output:
        output = run_and_capture(display_assets, data)
        markdown_output = output.replace('\x1b[36m- ', '- ').replace('\x1b[0m', '')  # Removing the color codes
        pyperclip.copy(markdown_output)
    else:
        display_assets(data)