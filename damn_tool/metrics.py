import boto3
from botocore.exceptions import ClientError
import click
import datetime
import json
import pyperclip
import requests
from termcolor import colored

from .utils.helpers import load_config, run_and_capture, format_size
from .utils.aws import list_objects_and_folders


def get_dagster_metrics(asset, profile):
    # Getting and processing Dagster metrics...
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

    run_id = 'N/A'
    status = 'N/A'
    start_time = 'N/A'
    end_time = 'N/A'
    elapsed_time = 'N/A'
    num_partitions = 'N/A'
    num_materialized = 'N/A'
    num_failed = 'N/A'

    asset_info = data["data"]["assetOrError"]

    # Get AssetMaterializations attributes
    if asset_info['assetMaterializations']:
        first_materialization = asset_info['assetMaterializations'][0]
        run_id = first_materialization['runId'] if 'runId' in first_materialization else 'N/A'

        # Extract stepStats if available
        step_stats = first_materialization['stepStats'] if 'stepStats' in first_materialization else {}
        status = step_stats['status'] if 'status' in step_stats else 'N/A'
        
        if 'startTime' in step_stats:
            start_time = datetime.datetime.fromtimestamp(step_stats['startTime']).strftime('%Y-%m-%d %H:%M:%S')

        if 'endTime' in step_stats:
            end_time = datetime.datetime.fromtimestamp(step_stats['endTime']).strftime('%Y-%m-%d %H:%M:%S')

        # Calculate and format elapsed time
        if 'startTime' in step_stats and 'endTime' in step_stats:
            elapsed_seconds = step_stats['endTime'] - step_stats['startTime']
            elapsed_time = str(datetime.timedelta(seconds=elapsed_seconds))
                    
    # Get Definition attributes
    if asset_info['definition']:
        definition = asset_info['definition']
        
        if 'partitionStats' in definition and definition['partitionStats'] is not None:
            partition_stats = definition['partitionStats']
            num_partitions = partition_stats['numPartitions'] if 'numPartitions' in partition_stats else 'N/A'
            num_materialized = partition_stats['numMaterialized'] if 'numMaterialized' in partition_stats else 'N/A'
            num_failed = partition_stats['numFailed'] if 'numFailed' in partition_stats else 'N/A'

    return {
        'run_id': run_id,
        'status': status,
        'start_time': start_time,
        'end_time': end_time,
        'elapsed_time': elapsed_time,
        'num_partitions': num_partitions,
        'num_materialized': num_materialized,
        'num_failed': num_failed
    }


def get_io_manager_metrics(asset, io_manager):
    io_manager_config = load_config('io-manager', io_manager)

    # Configure boto to use your credentials
    boto3.setup_default_session(aws_access_key_id=io_manager_config['credentials']['access_key_id'], 
                                aws_secret_access_key=io_manager_config['credentials']['secret_access_key'])
    
    s3 = boto3.client('s3')

    # Get S3 items with that asset name
    s3_items = list_objects_and_folders(io_manager_config['bucket_name'], io_manager_config['key_prefix'] + "/" + asset)
    
    return {
        'files': s3_items[0]['num_files'],
        'size': s3_items[0]['file_size'],
        'last_modified': s3_items[0]['last_modified_ts']
    }


def display_metrics(dagster_metrics, io_manager_metrics):
    click.echo(colored("Latest Dagster materialization metrics:", 'magenta'))
    click.echo(colored(f"- Latest run ID: ", 'yellow') + colored(f"{dagster_metrics['run_id']}", 'green'))
    click.echo(colored(f"- Status: ", 'yellow') + colored(f"{dagster_metrics['status']}", 'green'))
    click.echo(colored(f"- Start time: ", 'yellow') + colored(f"{dagster_metrics['start_time']}", 'green'))
    click.echo(colored(f"- End time: ", 'yellow') + colored(f"{dagster_metrics['end_time']}", 'green'))
    click.echo(colored(f"- Elapsed time: ", 'yellow') + colored(f"{dagster_metrics['elapsed_time']}", 'green'))

    click.echo('\n')

    click.echo(colored("Dagster partitions:", 'magenta'))
    click.echo(colored(f"- Number of partitions: ", 'yellow') + colored(f"{dagster_metrics['num_partitions']}", 'green'))
    click.echo(colored(f"- Materialized partitions: ", 'yellow') + colored(f"{dagster_metrics['num_materialized']}", 'green'))
    click.echo(colored(f"- Failed partitions: ", 'yellow') + colored(f"{dagster_metrics['num_failed']}", 'green'))

    click.echo('\n')

    click.echo(colored("IO Manager:", 'magenta'))
    click.echo(colored(f"- Files: ", 'yellow') + colored(f"{io_manager_metrics['files']}", 'green'))
    click.echo(colored(f"- File(s) size: ", 'yellow') + colored(format_size(io_manager_metrics['size']), 'green'))
    click.echo(colored(f"- Last modified: ", 'yellow') + colored(f"{io_manager_metrics['last_modified']}", 'green'))


@click.command()
@click.argument('asset', type=str)
@click.option('--profile', default='prod', help='Profile to use')
@click.option('--io_manager', default='aws', help='IO manager storage system to use')
@click.option('--copy-output', is_flag=True, help='Copy command output to clipboard')
def metrics(asset, profile, io_manager, copy_output):
    """List your asset's metrics"""
    dagster_metrics = get_dagster_metrics(asset, profile)
    io_manager_metrics = get_io_manager_metrics(asset, io_manager)

    if copy_output:
        output = run_and_capture(display_metrics, dagster_metrics, io_manager_metrics)
        markdown_output = output.replace('\x1b[36m- ', '- ').replace('\x1b[0m', '')  # Removing the color codes
        pyperclip.copy(markdown_output)
    else:
        display_metrics(dagster_metrics, io_manager_metrics)