import click
import json
import pyperclip

from .utils.helpers import (
    init_connectors,
    package_command_output, 
    print_packaged_command_output, 
    run_and_capture
)


def get_orchestrator_data(orchestrator_connector, prefix):
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

    result = orchestrator_connector.execute(query)
    
    return result


@click.command()
@click.option('--prefix', default=None, help='Get list of assets with a given prefix')
@click.option('--orchestrator', default=None, help='Orchestrator service provider to use')
@click.option('--io_manager', default=None, help='IO manager service provider to use')
@click.option('--data-warehouse', default=None, help='Data warehouse service provider to use')
@click.option('--output', default='terminal', help='Destination for command output. Options include `terminal` (default) for standard output, `json` to format output as JSON, or `copy` to copy the output to the clipboard.')
def ls(prefix, orchestrator, io_manager, data_warehouse, output):
    """List your platform's data assets"""
    # Initialize connectors
    orchestrator_connector, io_manager_connector, data_warehouse_connector = init_connectors(orchestrator, io_manager, data_warehouse)

    # Get assets
    orchestrator_data = get_orchestrator_data(orchestrator_connector, prefix) if orchestrator_connector else None

    # Package and output assets
    packaged_command_output = package_command_output('ls', orchestrator_data)

    if output == 'json':
        print(packaged_command_output)
    elif output == 'copy':
        print_output = run_and_capture(print_packaged_command_output, packaged_command_output)
        markdown_output = print_output.replace('\x1b[36m- ', '- ').replace('\x1b[0m', '')  # Removing the color codes
        pyperclip.copy(markdown_output)
    else:
        print_packaged_command_output(packaged_command_output)