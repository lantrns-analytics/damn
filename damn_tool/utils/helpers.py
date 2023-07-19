import click
import datetime
import io
from jinja2 import Environment, FileSystemLoader, select_autoescape
import json
import os
import sys
from termcolor import colored
import yaml

from .adapters.orchestrators.dagster import DagsterAdapter
from .adapters.io_managers.aws import AWSAdapter
from .adapters.data_warehouses.snowflake import SnowflakeAdapter

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)


def format_size(size):
    # if size is None, return a default value
    if size is None:
        return "N/A"

    # Size must be bytes
    units = ['bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    unit = 0

    while size >= 1024 and unit < len(units)-1:
        # shift to the next unit
        size /= 1024
        unit += 1

    return f"{size:.2f} {units[unit]}"


def init_connectors(orchestrator, io_manager, data_warehouse):
    # Initiate orchestrator
    orchestrator_connector_type, orchestrator_config = load_config('orchestrator', orchestrator)

    if orchestrator_connector_type == 'dagster':
        orchestrator_connector = DagsterAdapter(orchestrator_config)
    else:
        orchestrator_connector = None
    
    # Initiate IO manager
    io_manager_connector_type, io_manager_config = load_config('io-manager', io_manager)

    if io_manager_connector_type == 'aws':
        io_manager_connector = AWSAdapter(io_manager_config)
    else:
        io_manager_connector = None

    # Initiate data warehouse connector
    data_warehouse_connector_type, data_warehouse_config = load_config('data-warehouse', data_warehouse)

    if data_warehouse_connector_type == 'snowflake':
        data_warehouse_connector = SnowflakeAdapter(data_warehouse_config)
    else:
        data_warehouse_connector = None
    
    return orchestrator_connector, io_manager_connector, data_warehouse_connector
    

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
            return first_key, config[connector][first_key]
        else:
            return profile, config[connector][profile]
    except KeyError:
        return None, None


def package_command_output(command, data):
    packaged_command_output = {}
    
    if command == 'ls':
        ls_items = []
        for node in data['data']['assetsOrError']['nodes']:
            asset_key = "/".join(node['key']['path'])
            ls_items.append(asset_key)
        
        packaged_command_output['ls'] = ls_items
    
    elif command == 'show':
        show_info = {
            "From orchestrator": data['Orchestrator Attributes'],
            "From data warehouse": data['Data Warehouse Attributes']
        }
        packaged_command_output = {command: show_info}
    
    elif command == 'metrics':
        if data['IO Manager Metrics'] is not None:
            data['IO Manager Metrics']['size'] = format_size(data['IO Manager Metrics']['size'])
        if data['Data Warehouse Metrics'] is not None:
            data['Data Warehouse Metrics']['bytes'] = format_size(data['Data Warehouse Metrics']['bytes'])

        metrics_info = {
            "From orchestrator": data['Orchestrator Metrics'],
            "From IO manager": data['IO Manager Metrics'],
            "From data warehouse": data['Data Warehouse Metrics']
        }
        packaged_command_output = {command: metrics_info}

    return json.dumps(packaged_command_output, cls=DateTimeEncoder)


def print_packaged_command_output(packaged_display_items, level=-1):
    # Load the JSON string to a dict if it's a string
    if isinstance(packaged_display_items, str):
        packaged_display_items = json.loads(packaged_display_items)

    # Define indent for readability
    indent = ' ' * max(0, level)

    # Check if the current item is a dictionary
    if isinstance(packaged_display_items, dict):
        for key, value in packaged_display_items.items():
            if isinstance(value, (dict, list)):
                if level >= 0:  # Avoid printing the top level key
                    # If the key is a dict and we are at the first level, don't print the dash
                    dash = "" if level == -1 else "-"
                    prefix = f"{dash} {key}:" if level > 0 else f"{key}:"
                    click.echo(colored(f"{indent}{prefix}", 'yellow' if level > 1 else 'magenta').lstrip())
                print_packaged_command_output(value, level=level + 1)
            elif level >= 0:  # Avoid printing the top level key-value pairs
                click.echo(colored(f"{indent}- {key}: ", 'yellow') + colored(f"{value}", 'green'))

    # Check if the current item is a list
    elif isinstance(packaged_display_items, list):
        for value in packaged_display_items:
            if isinstance(value, (dict, list)):
                print_packaged_command_output(value, level=level + 1)
            elif level >= 0:  # Avoid printing the top level list items
                click.echo(colored(f"{indent}- {value}", 'cyan'))


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