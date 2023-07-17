import click
import json
import pyperclip
import requests
from termcolor import colored

from .utils.helpers import load_config, run_and_capture


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


def display_assets(data, output):
    # Extract asset keys and print them
    if output == 'terminal':
      for node in data['data']['assetsOrError']['nodes']:
          asset_key = "/".join(node['key']['path'])
          click.echo(colored(f'- {asset_key}', 'cyan'))


@click.command()
@click.option('--prefix', default=None, help='Get list of assets with a given prefix')
@click.option('--profile', default=None, help='Profile to use')
@click.option('--output', default='terminal', help='Destination for command output. Options include `terminal` (default) for standard output, `json` to format output as JSON, or `copy` to copy the output to the clipboard.')
def ls(prefix, profile, output):
    """List your platform's data assets"""
    data = get_orchestrator_assets(prefix, profile)

    if output == 'copy':
        output = run_and_capture(display_assets, data, 'terminal')
        markdown_output = output.replace('\x1b[36m- ', '- ').replace('\x1b[0m', '')  # Removing the color codes
        pyperclip.copy(markdown_output)
    else:
        display_assets(data, output)