import click
import datetime
import io
from jinja2 import Environment, FileSystemLoader, select_autoescape
import json
import os
import sys
from termcolor import colored
import yaml

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)
    

def load_config(connector, profile):
    # Create the Jinja environment and register the os.getenv function
    env = Environment(loader=FileSystemLoader(os.path.expanduser('~/.damn')), autoescape=select_autoescape(['yaml']))
    env.globals['env'] = os.getenv

    # Render the template
    template = env.get_template('connectors.yml')
    rendered_template = template.render()

    # Load the YAML
    config = yaml.safe_load(rendered_template)

    try:
        if not profile:
            first_key = list(config[connector].keys())[0]
            return config[connector][first_key]
        else:
            return config[connector][profile]
    except KeyError:
        raise ValueError(f"No configuration found for connector '{connector}' with profile '{profile}'")


def package_command_output(command, data):
    packaged_command_output = {}
    
    if command == 'ls':
        ls_items = []
        for node in data['data']['assetsOrError']['nodes']:
            asset_key = "/".join(node['key']['path'])
            ls_items.append(asset_key)
        
        packaged_command_output['ls'] = ls_items
    
    elif command == 'show':
        asset_info = data["data"]["assetOrError"]
        # Create a dictionary for 'show' command
        show_info = {}

        freshness_policy = asset_info['definition'].get('freshnessPolicy', None)
        if freshness_policy is not None:
            freshness_policy_lag = freshness_policy.get('maximumLagMinutes', 'Not available')
            freshness_policy_cron = freshness_policy.get('cronSchedule', 'Not available')
        else:
            freshness_policy_lag = 'Not available'
            freshness_policy_cron = 'Not available'

        show_info["Asset attributes"] = {
            "description": asset_info['definition'].get('description', 'Not available'),
            "compute_kind": asset_info['definition'].get('computeKind', 'Not available'),
            "is_partitioned": asset_info['definition'].get('isPartitioned', 'Not available'),
            "auto_materialization_policy": asset_info['definition'].get('autoMaterializePolicy', {}).get('policyType', 'Not available') if asset_info['definition'].get('autoMaterializePolicy', None) is not None else 'Not available',
            "freshness_policy_lag": freshness_policy_lag,
            "freshness_policy_cron": freshness_policy_cron,
        }

        show_info["Upstream assets"] = ["/".join(path['path']) for path in asset_info['definition']['dependencyKeys']]
        show_info["Downstream assets"] = ["/".join(path['path']) for path in asset_info['definition']['dependedByKeys']]

        show_info["Latest materialization's metadata entries"] = {}
        if asset_info.get('assetMaterializations', []):
            last_materialization = asset_info['assetMaterializations'][0]
            show_info["Latest materialization's metadata entries"]["Last materialization timestamp"] = last_materialization.get('timestamp', 'Not available')

            # handle metadata entries
            metadata_entries_dict = {}
            if last_materialization.get('metadataEntries', []):
                for entry in last_materialization['metadataEntries']:
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

                    metadata_entries_dict[label] = value

            show_info["Latest materialization's metadata entries"]["metadata_entries"] = metadata_entries_dict

        packaged_command_output['show'] = show_info
    
    elif command == 'metrics':
        data['IO Manager Metrics']['size'] = format_size(data['IO Manager Metrics']['size'])

        metrics_info = {
            "Latest Orchestrator materialization metrics": data['Orchestrator Metrics'],
            "IO Manager": data['IO Manager Metrics'],
            "Data Warehouse": data['Data Warehouse Metrics']
        }
        packaged_command_output = {command: metrics_info}

    return json.dumps(packaged_command_output, cls=DateTimeEncoder)


def print_packaged_command_output(packaged_display_items, level=0):
    # Load the JSON string to a dict if it's a string
    if isinstance(packaged_display_items, str):
        packaged_display_items = json.loads(packaged_display_items)

    # Check if the current item is a dictionary
    if isinstance(packaged_display_items, dict):
        for key, value in packaged_display_items.items():
            # If the value is a dictionary or a list, print the key as a title and recursively print the value
            if isinstance(value, (dict, list)):
                # Top level keys are not printed
                if int(level) > 0:
                    click.echo(colored(f"{key}:", 'magenta'))
                print_packaged_command_output(value, level=level + 1)
            # If the value is not a dictionary or a list, print the key-value pair
            else:
                click.echo(colored(f"- {key}: ", 'yellow') + colored(f"{value}", 'green'))
    # Check if the current item is a list
    elif isinstance(packaged_display_items, list):
        for value in packaged_display_items:
            # If the value is a dictionary or a list, recursively print the value
            if isinstance(value, (dict, list)):
                print_packaged_command_output(value, level=level + 1)
            # If the value is not a dictionary or a list, print the value
            else:
                click.echo(colored(f"- {value}", 'cyan'))



def run_and_capture(func, *args, **kwargs):
    # Create a string buffer and redirect stdout to it
    buffer = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = buffer

    # Run the function and capture its output
    try:
        func(*args, **kwargs)
    finally:
        # Ensure the original stdout is restored even if the function raises an exception
        sys.stdout = old_stdout

    return buffer.getvalue()


def format_size(size):
    # size is in bytes
    units = ['Bytes', 'KB', 'MB', 'GB', 'TB']
    unit = 0
    while size >= 1024 and unit < len(units)-1:
        size /= 1024
        unit += 1
    return f"{size:.2f} {units[unit]}"