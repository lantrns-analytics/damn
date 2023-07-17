import click
import io
from jinja2 import Environment, FileSystemLoader, select_autoescape
import json
import os
import sys
from termcolor import colored
import yaml

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
    packaged_display_items = {}
    
    if command == 'ls':
        ls_items = []
        for node in data['data']['assetsOrError']['nodes']:
            asset_key = "/".join(node['key']['path'])
            ls_items.append(asset_key)
        
        packaged_display_items['ls'] = ls_items

    return json.dumps(packaged_display_items)


def print_packaged_command_output(packaged_display_items):
    # Load the JSON string to a dict if it's a string
    if isinstance(packaged_display_items, str):
        packaged_display_items = json.loads(packaged_display_items)
        
    # Extract asset keys and print them
    if 'ls' in packaged_display_items:
        for asset_key in packaged_display_items['ls']:
            click.echo(colored(f'- {asset_key}', 'cyan'))


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