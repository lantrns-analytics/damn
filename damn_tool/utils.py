import yaml
import os

def load_config(connector, profile):
    with open(os.path.expanduser('~/.damn/connectors.yml'), 'r') as file:
        config = yaml.safe_load(file)
    try:
        return config[connector][profile]
    except KeyError:
        raise ValueError(f"No configuration found for connector '{connector}' with profile '{profile}'")

