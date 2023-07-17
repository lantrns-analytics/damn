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


def get_orchestrator_asset_info(asset, profile):
    # Get connector configs
    orchestrator_config = load_config('orchestrator', profile)

    # Set headers
    headers = {
        "Content-Type": "application/json",
        "Dagster-Cloud-Api-Token": orchestrator_config['api_token'],
    }

    # Split the asset key into a list of strings
    asset_key = asset.split("/")

    # Get data
    query = f"""
    query AssetByKey {{
      assetOrError(assetKey: {{path: {json.dumps(asset_key)}}}) {{
        __typename
        ... on Asset {{
          definition {{
            description
            computeKind
            autoMaterializePolicy{{
              policyType
            }}
            freshnessPolicy{{
              maximumLagMinutes
              cronSchedule
            }}
            isPartitioned
            dependedByKeys{{
              path
            }}
            dependencyKeys{{
              path
            }}
          }}
          assetMaterializations(limit: 1){{
            timestamp
            metadataEntries{{
              __typename
              ... on FloatMetadataEntry{{
                label
                description
                floatValue
              }}
              ... on IntMetadataEntry{{
                label
                description
                intValue
              }}
              ... on JsonMetadataEntry{{
                label
                description
                jsonString
              }}
              ... on BoolMetadataEntry{{
                label
                description
                boolValue
              }}
              ... on MarkdownMetadataEntry{{
                label
                description
                mdStr
              }}
              ... on PathMetadataEntry{{
                label
                description
                path
              }}
              ... on NotebookMetadataEntry{{
                label
                description
                path
              }}
              ... on PythonArtifactMetadataEntry{{
                label
                description
                module
                name
              }}
              ... on TextMetadataEntry{{
                label
                description
                text
              }}
              ... on UrlMetadataEntry{{
                label
                description
                url
              }}
              ... on PipelineRunMetadataEntry{{
                label
                description
                runId
              }}
              ... on AssetMetadataEntry{{
                label
                description
                assetKey{{
                  path
                }}
              }}
              ... on NullMetadataEntry{{
                label
                description
              }}
            }}
          }}
        }}
        ... on AssetNotFoundError {{
          message
        }}
      }}
    }}
    """

    response = requests.post(
        orchestrator_config['endpoint'], # type: ignore
        headers=headers,
        json={"query": query}
    )
    
    response.raise_for_status()
    
    return response.json()


@click.command()
@click.argument('asset', required=True)
@click.option('--profile', default=None, help='Profile to use')
@click.option('--output', default='terminal', help='Destination for command output. Options include `terminal` (default) for standard output, `json` to format output as JSON, or `copy` to copy the output to the clipboard.')
def show(asset, profile, output):
    """Show details for a specific asset"""
    data = get_orchestrator_asset_info(asset, profile)
    packaged_command_output = package_command_output('show', data)

    if output == 'json':
        print(packaged_command_output)
    elif output == 'copy':
        print_output = run_and_capture(print_packaged_command_output, packaged_command_output)
        markdown_output = print_output.replace('\x1b[36m- ', '- ').replace('\x1b[0m', '')  # Removing the color codes
        pyperclip.copy(markdown_output)
    else:
        print_packaged_command_output(packaged_command_output)