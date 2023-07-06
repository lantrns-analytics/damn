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


@click.group()
def cli():
    pass


@cli.command()
@click.option('--prefix', default=None, help='Get list of assets with a given prefix')
@click.option('--asset', default=None, help='Get asset definition and dependancies')
def ls(prefix, asset):
    """List your platform's data assets"""
    if asset:
        asset_details(asset)
    else:
        list_assets(prefix)


def list_assets(prefix):
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
        headers=headers,
        json={"query": query}
    )
    
    response.raise_for_status()
    
    data = response.json()

    # Extract asset keys and print them
    for node in data['data']['assetsOrError']['nodes']:
        asset_key = "/".join(node['key']['path'])
        click.echo(f'- {asset_key}')


def asset_details(asset):
    """Print details for a single asset."""
    # Split the asset key into a list of strings
    asset_key = asset.split("/")

    # Insert asset_key directly into the query using f-string formatting
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
        }}
        ... on AssetNotFoundError {{
          message
        }}
      }}
    }}
    """

    response = requests.post(
        GRAPHQL_URL, # type: ignore
        headers=headers,
        json={"query": query}
    )
    
    response.raise_for_status()
    
    data = response.json()

    # Check if an error occurred and print the error message or asset details
    if data["data"]["assetOrError"]["__typename"] == "AssetNotFoundError":
        click.echo(f"Error: {data['data']['assetOrError']['message']}")
    else:
        asset_info = data["data"]["assetOrError"]
        click.echo(f"Asset details for {asset}:\n")

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

        click.echo(colored(f"Description: ", 'yellow') + colored(f"{description}", 'green'))
        click.echo(colored(f"Compute kind: ", 'yellow') + colored(f"{compute_kind}", 'green'))
        click.echo(colored(f"Is partitioned: ", 'yellow') + colored(f"{is_partitioned}", 'green'))
        click.echo(colored(f"Auto-materialization policy: ", 'yellow') + colored(f"{auto_materialize_policy}", 'green'))
        click.echo(colored(f"Freshess policy (maximum lag minutes): ", 'yellow') + colored(f"{freshness_policy_lag}", 'green'))
        click.echo(colored(f"Freshess policy (cron schedule): ", 'yellow') + colored(f"{freshness_policy_cron}", 'green'))

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




if __name__ == '__main__':
    cli()