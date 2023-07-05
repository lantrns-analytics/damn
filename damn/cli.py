import os
import json
import click
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

GRAPHQL_URL = "https://discursus.dagster.cloud/prod/graphql"
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
@click.option('--asset', default=None, help='Get asset definition items and dependancies')
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
        GRAPHQL_URL,
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
        GRAPHQL_URL,
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
        auto_materialize_policy = asset_info['definition']['autoMaterializePolicy'].get('policyType', 'Not available')

        # Check if freshnessPolicy is not None before accessing its fields
        freshness_policy = asset_info['definition'].get('freshnessPolicy', {})
        freshness_policy_lag = freshness_policy.get('maximumLagMinutes', 'Not available') if freshness_policy is not None else 'Not available'
        freshness_policy_cron = freshness_policy.get('cronSchedule', 'Not available') if freshness_policy is not None else 'Not available'

        click.echo(f"Description: {description}")
        click.echo(f"Compute kind: {compute_kind}")
        click.echo(f"Is partitioned: {is_partitioned}")
        click.echo(f"Auto-materialization policy: {auto_materialize_policy}")
        click.echo(f"Freshess policy (maximum lag minutes): {freshness_policy_lag}")
        click.echo(f"Freshess policy (cron schedule): {freshness_policy_cron}")

        click.echo("\nUpstream assets:")
        upstream_assets = asset_info['definition']['dependencyKeys']
        if upstream_assets:
            for path in upstream_assets:
                click.echo(f"- {'/'.join(path['path'])}")
        else:
            click.echo("- None")

        click.echo("\nDownstream assets:")
        downstream_assets = asset_info['definition']['dependedByKeys']
        if downstream_assets:
            for path in downstream_assets:
                click.echo(f"- {'/'.join(path['path'])}")
        else:
            click.echo("- None")




if __name__ == '__main__':
    cli()