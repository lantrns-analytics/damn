from jinja2 import Environment, FileSystemLoader, select_autoescape
import os
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
