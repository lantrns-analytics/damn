import click
import json
import pyperclip
import requests

from .utils.helpers import (
    load_config, 
    package_command_output, 
    print_packaged_command_output, 
    run_and_capture
)


def get_orchestrator_assets(prefix, orchestrator):
    # Get connector configs
    orchestrator_config = load_config('orchestrator', orchestrator)

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


@click.command()
@click.option('--prefix', default=None, help='Get list of assets with a given prefix')
@click.option('--orchestrator', default=None, help='Orchestrator service provider to use')
@click.option('--output', default='terminal', help='Destination for command output. Options include `terminal` (default) for standard output, `json` to format output as JSON, or `copy` to copy the output to the clipboard.')
def ls(prefix, orchestrator, output):
    """List your platform's data assets"""
    data = get_orchestrator_assets(prefix, orchestrator)
    packaged_command_output = package_command_output('ls', data)

    if output == 'json':
        print(packaged_command_output)
    elif output == 'copy':
        print_output = run_and_capture(print_packaged_command_output, packaged_command_output)
        markdown_output = print_output.replace('\x1b[36m- ', '- ').replace('\x1b[0m', '')  # Removing the color codes
        pyperclip.copy(markdown_output)
    else:
        print_packaged_command_output(packaged_command_output)