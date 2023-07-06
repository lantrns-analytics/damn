import yaml
import os

def load_config(connector, profile):
    with open(os.path.expanduser('~/.damn/connectors.yml'), 'r') as file:
        config = yaml.safe_load(file)
    return config.get(connector, {}).get(profile, None)
