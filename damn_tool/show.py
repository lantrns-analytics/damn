import click
import json
import pyperclip
import requests
from termcolor import colored

from .utils.helpers import load_config, run_and_capture


def get_dagster_asset_info(asset, profile):
    # Get connector configs
    dagster_config = load_config('dagster', profile)

    # Set headers
    headers = {
        "Content-Type": "application/json",
        "Dagster-Cloud-Api-Token": dagster_config['api_token'],
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
        dagster_config['endpoint'], # type: ignore
        headers=headers,
        json={"query": query}
    )
    
    response.raise_for_status()
    
    return response.json()


def display_asset_info(asset, data):
    if data["data"]["assetOrError"]["__typename"] == "AssetNotFoundError":
        click.echo(f"Error: {data['data']['assetOrError']['message']}")
    else:
        asset_info = data["data"]["assetOrError"]
        # Use the get method to safely access the keys in the dictionary
        description = asset_info['definition'].get('description', 'Not available')
        compute_kind = asset_info['definition'].get('computeKind', 'Not available')
        is_partitioned = asset_info['definition'].get('isPartitioned', 'Not available')
        auto_materialize_policy_obj = asset_info['definition'].get('autoMaterializePolicy', None)
        auto_materialize_policy = auto_materialize_policy_obj.get('policyType', 'Not available') if auto_materialize_policy_obj is not None else 'Not available'

        # Check if freshnessPolicy is not None before accessing its fields
        freshness_policy = asset_info['definition'].get('freshnessPolicy', {})
        freshness_policy_lag = freshness_policy.get('maximumLagMinutes', 'Not available') if freshness_policy is not None else 'Not available'
        freshness_policy_cron = freshness_policy.get('cronSchedule', 'Not available') if freshness_policy is not None else 'Not available'

        click.echo(colored("Asset attributes:", 'magenta'))
        click.echo(colored(f"- Key: ", 'yellow') + colored(f"{asset}", 'green'))
        click.echo(colored(f"- Description: ", 'yellow') + colored(f"{description}", 'green'))
        click.echo(colored(f"- Compute kind: ", 'yellow') + colored(f"{compute_kind}", 'green'))
        click.echo(colored(f"- Is partitioned: ", 'yellow') + colored(f"{is_partitioned}", 'green'))
        click.echo(colored(f"- Auto-materialization policy: ", 'yellow') + colored(f"{auto_materialize_policy}", 'green'))
        click.echo(colored(f"- Freshess policy (maximum lag minutes): ", 'yellow') + colored(f"{freshness_policy_lag}", 'green'))
        click.echo(colored(f"- Freshess policy (cron schedule): ", 'yellow') + colored(f"{freshness_policy_cron}", 'green'))

        click.echo(colored("\nUpstream assets:", 'magenta'))
        upstream_assets = asset_info['definition']['dependencyKeys']
        if upstream_assets:
            for path in upstream_assets:
                click.echo(colored(f"- {'/'.join(path['path'])}", 'cyan'))
        else:
            click.echo("- None")

        click.echo(colored("\nDownstream assets:", 'magenta'))
        downstream_assets = asset_info['definition']['dependedByKeys']
        if downstream_assets:
            for path in downstream_assets:
                click.echo(colored(f"- {'/'.join(path['path'])}", 'cyan'))
        else:
            click.echo("- None")
        
        # Handle 'assetMaterializations'
        click.echo(colored("\nLatest materialization's metadata entries:", 'magenta'))
        asset_materializations = asset_info.get('assetMaterializations', [])
        if asset_materializations:
            # Get the most recent materialization (it should be the first since we limited it to 1)
            last_materialization = asset_materializations[0]

            timestamp = last_materialization.get('timestamp', 'Not available')
            click.echo(colored(f"- Last materialization timestamp: ", 'yellow') + colored(f"{timestamp}", 'green'))
            
            # Handle 'metadataEntries'
            metadata_entries = last_materialization.get('metadataEntries', [])
            if metadata_entries:
                for entry in metadata_entries:
                    label = entry.get('label', 'Not available')
                    description = entry.get('description', 'Not available')
                    typename = entry.get('__typename')

                    value = 'Not available'
                    if typename == 'FloatMetadataEntry':
                        value = entry.get('floatValue', 'Not available')
                    elif typename == 'IntMetadataEntry':
                        value = entry.get('intValue', 'Not available')
                    elif typename == 'JsonMetadataEntry':
                        value = entry.get('jsonString', 'Not available')
                    elif typename == 'BoolMetadataEntry':
                        value = entry.get('boolValue', 'Not available')
                    elif typename == 'MarkdownMetadataEntry':
                        value = entry.get('mdStr', 'Not available')
                    elif typename == 'PathMetadataEntry' or typename == 'NotebookMetadataEntry':
                        value = entry.get('path', 'Not available')
                    elif typename == 'PythonArtifactMetadataEntry':
                        value = f"module: {entry.get('module', 'Not available')}, name: {entry.get('name', 'Not available')}"
                    elif typename == 'TextMetadataEntry':
                        value = entry.get('text', 'Not available')
                    elif typename == 'UrlMetadataEntry':
                        value = entry.get('url', 'Not available')
                    elif typename == 'PipelineRunMetadataEntry':
                        value = entry.get('runId', 'Not available')
                    elif typename == 'AssetMetadataEntry':
                        asset_key_path = entry.get('assetKey', {}).get('path', 'Not available')
                        value = '/'.join(asset_key_path) if asset_key_path != 'Not available' else 'Not available'
                    elif typename == 'NullMetadataEntry':
                        value = 'Null'

                    click.echo(colored(f"- {label}: ", 'yellow') + colored(f"{value}", 'green'))
            else:
                click.echo(colored(f"- No metadata entries.", 'yellow'))
        else:
            click.echo(colored(f"- No asset materializations.", 'yellow'))


@click.command()
@click.argument('asset', required=True)
@click.option('--profile', default='prod', help='Profile to use')
@click.option('--copy-output', is_flag=True, help='Copy command output to clipboard')
def show(asset, profile, copy_output):
    """Show details for a specific asset"""
    data = get_dagster_asset_info(asset, profile)

    if copy_output:
        output = run_and_capture(display_asset_info, asset, data)
        markdown_output = output.replace('\x1b[36m- ', '- ').replace('\x1b[0m', '')  # Removing the color codes
        pyperclip.copy(markdown_output)
    else:
        display_asset_info(asset, data)