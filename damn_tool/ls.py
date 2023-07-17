import click
import json
import pyperclip
import requests
from termcolor import colored

from .utils.helpers import load_config, package_display_items, run_and_capture


def get_orchestrator_assets(prefix, profile):
    # Get connector configs
    orchestrator_config = load_config('orchestrator', profile)

    # Set headers
    headers = {
        "Content-Type": "application/json",
        "Dagster-Cloud-Api-Token": orchestrator_config['api_token'],
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
        orchestrator_config['endpoint'], # type: ignore
        headers=headers, # type: ignore
        json={"query": query}
    )
    
    response.raise_for_status()
    
    return response.json()


def print_assets(packaged_display_items):
    # Load the JSON string to a dict if it's a string
    if isinstance(packaged_display_items, str):
        packaged_display_items = json.loads(packaged_display_items)
        
    # Extract asset keys and print them
    if 'ls' in packaged_display_items:
        for asset_key in packaged_display_items['ls']:
            click.echo(colored(f'- {asset_key}', 'cyan'))


@click.command()
@click.option('--prefix', default=None, help='Get list of assets with a given prefix')
@click.option('--profile', default=None, help='Profile to use')
@click.option('--output', default='terminal', help='Destination for command output. Options include `terminal` (default) for standard output, `json` to format output as JSON, or `copy` to copy the output to the clipboard.')
def ls(prefix, profile, output):
    """List your platform's data assets"""
    data = get_orchestrator_assets(prefix, profile)
    packaged_display_items = package_display_items('ls', data)

    if output == 'json':
        print(packaged_display_items)
    elif output == 'copy':
        output = run_and_capture(print_assets, packaged_display_items)
        markdown_output = output.replace('\x1b[36m- ', '- ').replace('\x1b[0m', '')  # Removing the color codes
        pyperclip.copy(markdown_output)
    else:
        print_assets(packaged_display_items)