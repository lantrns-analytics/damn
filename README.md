    ████████▄     ▄████████   ▄▄▄▄███▄▄▄▄   ███▄▄▄▄   
    ███   ▀███   ███    ███ ▄██▀▀▀███▀▀▀██▄ ███▀▀▀██▄ 
    ███    ███   ███    ███ ███   ███   ███ ███   ███ 
    ███    ███   ███    ███ ███   ███   ███ ███   ███ 
    ███    ███ ▀███████████ ███   ███   ███ ███   ███ 
    ███    ███   ███    ███ ███   ███   ███ ███   ███ 
    ███   ▄███   ███    ███ ███   ███   ███ ███   ███ 
    ████████▀    ███    █▀   ▀█   ███   █▀   ▀█   █▀                                                 

# Data Asset Metrics Navigator
The DAMN tool extracts and reports metrics about your data assets.

It allows you to inspect your assets, lineage, and all sorts of metrics around materialization, usage, physical space usage and query performance. The objective of the DAMN tool is to give you a convenient command-line tool to track and report on the data assets you're working on.

## Installation
To install the DAMN tool, run the following command:

```bash
pip install damn-tool
```

<br/><br/>


## Connector Configuration
The DAMN tool leverages various connectors to interact with different data systems. Configuring these connectors is done via a YAML file located at `~/.damn/connectors.yml`.

### YAML Configuration Structure
The configuration file uses the following structure:

```yaml
connector_name:
  profile_name:
    param1: value1
    param2: value2
```

- connector_name: The name of the connector (e.g., dagster, dbt, s3, etc.).
- profile_name: The name of the profile for the connector. You can have multiple profiles per connector (e.g., prod, dev, test, etc.).
- param1, param2, etc.: The parameters needed for each connector. The required parameters will depend on the specific connector. For example, a Dagster connector might require endpoint and api_token.
### Switching Between Profiles
The active profile for each connector can be changed by specifying the profile when running damn commands. By default, damn will use the prod profile if no profile is specified.

Example usage:

```bash
damn ls --profile dev
```

<br/><br/>


## Connectors
### Dagster
This is for the moment the default and only connector supported by the DAMN tool. Here's an example configuration for a dagster connector with prod and dev profiles:

```yaml
dagster:
  prod:
    endpoint: https://your-dagster-instance.com/prod/graphql
    api_token: your-api-token
  dev:
    endpoint: https://your-dagster-instance.com/dev/graphql
    api_token: your-dev-api-token
```

<br/><br/>


## Usage
Here are some examples of how to use this CLI tool:

### List assets
```bash
foo@bar:~$ damn ls
```

<img src="https://raw.githubusercontent.com/discursus-data/damn/release/0.1/resources/images/damn_ls.png" width="500px" />

List all assets for a specifc key group
```bash
foo@bar:~$ damn ls --prefix gdelt
```

<img src="https://raw.githubusercontent.com/discursus-data/damn/release/0.1/resources/images/damn_ls_prefix.png" width="400px" />

### Show details for a specific asset
```bash
foo@bar:~$ damn show gdelt/gdelt_gkg_articles
```

<img src="https://raw.githubusercontent.com/discursus-data/damn/release/0.1/resources/images/damn_ls_asset.png" width="550px" />

<br/><br/>


## Contribution
Contributions to the DAMN tool are always welcome. Whether it's feature requests, bug fixes, or new features, your contribution is appreciated.

<br/><br/>


## License
The DAMN tool is open-source software, licensed under MIT.