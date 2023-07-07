import click
import datetime
import json
import requests
from termcolor import colored

from .utils import load_config 

@click.command()
@click.argument('asset', type=str)
@click.option('--profile', default='prod', help='Profile to use')
def metrics(asset, profile):
    """List your asset's metrics"""
    # Get connector configs
    dagster_config = load_config('dagster', profile)

    # Set headers
    headers = {
        "Content-Type": "application/json",
        "Dagster-Cloud-Api-Token": dagster_config['api_token'],
    }

    asset_list = asset.split('/')

    query = f"""
    query AssetMetricsByKey {{
        assetOrError(assetKey: {{path: {json.dumps(asset_list)}}}) {{
            __typename
            ... on Asset {{
                id
                assetMaterializations(limit: 1){{
                    runId
                    timestamp
                    stepStats{{
                        stepKey
                        status
                        startTime
                        endTime
                    }}
                }}
                definition{{
                    freshnessInfo{{
                        currentMinutesLate
                    }}
                    partitionStats{{
                        numPartitions
                        numMaterialized
                        numFailed
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
        headers=headers, # type: ignore
        json={"query": query}
    )
    
    response.raise_for_status()
    
    data = response.json()

    if data["data"]["assetOrError"]["__typename"] == "AssetNotFoundError":
        click.echo(f"Error: {data['data']['assetOrError']['message']}")
    else:
        asset_info = data["data"]["assetOrError"]
    
        # Get AssetMaterializations attributes
        if asset_info['assetMaterializations']:
            first_materialization = asset_info['assetMaterializations'][0]
            run_id = first_materialization['runId'] if 'runId' in first_materialization else 'Not available'

            # Extract stepStats if available
            step_stats = first_materialization['stepStats'] if 'stepStats' in first_materialization else {}
            status = step_stats['status'] if 'status' in step_stats else 'Not available'
            
            if 'startTime' in step_stats:
                start_time = datetime.datetime.fromtimestamp(step_stats['startTime']).strftime('%Y-%m-%d %H:%M:%S')
            else:
                start_time = 'Not available'

            if 'endTime' in step_stats:
                end_time = datetime.datetime.fromtimestamp(step_stats['endTime']).strftime('%Y-%m-%d %H:%M:%S')
            else:
                end_time = 'Not available'

            # Calculate and format elapsed time
            if 'startTime' in step_stats and 'endTime' in step_stats:
                elapsed_seconds = step_stats['endTime'] - step_stats['startTime']
                elapsed_time = str(datetime.timedelta(seconds=elapsed_seconds))
            else:
                elapsed_time = 'Not available'
                
        else:
            run_id = 'Not available'
            status = 'Not available'
            start_time = 'Not available'
            end_time = 'Not available'
            elapsed_time = 'Not available'
        
        # Get Definition attributes
        if asset_info['definition']:
            definition = asset_info['definition']
            
            if 'partitionStats' in definition and definition['partitionStats'] is not None:
                partition_stats = definition['partitionStats']
                num_partitions = partition_stats['numPartitions'] if 'numPartitions' in partition_stats else 'Not available'
                num_materialized = partition_stats['numMaterialized'] if 'numMaterialized' in partition_stats else 'Not available'
                num_failed = partition_stats['numFailed'] if 'numFailed' in partition_stats else 'Not available'
            else:
                num_partitions = 'Not available'
                num_materialized = 'Not available'
                num_failed = 'Not available'
        else:
            num_partitions = 'Not available'
            num_materialized = 'Not available'
            num_failed = 'Not available'

        click.echo('\n')

        click.echo(colored("Latest Dagster materialization metrics:", 'magenta'))
        click.echo(colored(f"- Latest run ID: ", 'yellow') + colored(f"{run_id}", 'green'))
        click.echo(colored(f"- Status: ", 'yellow') + colored(f"{status}", 'green'))
        click.echo(colored(f"- Start time: ", 'yellow') + colored(f"{start_time}", 'green'))
        click.echo(colored(f"- End time: ", 'yellow') + colored(f"{end_time}", 'green'))
        click.echo(colored(f"- Elapsed time: ", 'yellow') + colored(f"{elapsed_time}", 'green'))

        click.echo('\n')

        click.echo(colored("Dagster partitions:", 'magenta'))
        click.echo(colored(f"- Number of partitions: ", 'yellow') + colored(f"{num_partitions}", 'green'))
        click.echo(colored(f"- Materialized partitions: ", 'yellow') + colored(f"{num_materialized}", 'green'))
        click.echo(colored(f"- Failed partitions: ", 'yellow') + colored(f"{num_failed}", 'green'))

        click.echo('\n')