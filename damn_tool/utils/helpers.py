from jinja2 import Environment, FileSystemLoader, select_autoescape
import io
import os
import sys
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
        return config[connector][profile]
    except KeyError:
        raise ValueError(f"No configuration found for connector '{connector}' with profile '{profile}'")


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