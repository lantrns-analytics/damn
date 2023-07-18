import click
import json
import pyperclip

from .utils.helpers import (
    init_connectors,
    package_command_output, 
    print_packaged_command_output, 
    run_and_capture
)


def get_orchestrator_asset_info(orchestrator_connector, asset):
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

    result = orchestrator_connector.execute(query)
    
    return result


@click.command()
@click.pass_context
@click.argument('asset', required=True)
@click.option('--orchestrator', default=None, help='Orchestrator service provider to use')
@click.option('--data-warehouse', default=None, help='Data warehouse service provider to use')
@click.option('--output', default='terminal', help='Destination for command output. Options include `terminal` (default) for standard output, `json` to format output as JSON, or `copy` to copy the output to the clipboard.')
def show(ctx, asset, orchestrator, data_warehouse, output):
    """Show details for a specific asset"""
    # Initialize connectors
    orchestrator_connector, data_warehouse_connector = init_connectors(orchestrator, data_warehouse)

    # Get asset information
    data = get_orchestrator_asset_info(orchestrator_connector, asset)

    # Package and output asset information
    packaged_command_output = package_command_output('show', data)

    if output == 'json':
        print(packaged_command_output)
    elif output == 'copy':
        print_output = run_and_capture(print_packaged_command_output, packaged_command_output)
        markdown_output = print_output.replace('\x1b[36m- ', '- ').replace('\x1b[0m', '')  # Removing the color codes
        pyperclip.copy(markdown_output)
    else:
        print_packaged_command_output(packaged_command_output)