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

[*See example configuration file here*](connectors.yml.REPLACE)

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
This is currently the default connector supported by the DAMN tool. Here's an example configuration for a dagster connector with prod and dev profiles:

```yaml
dagster:
  prod:
    endpoint: https://your-dagster-instance.com/prod/graphql
    api_token: your-api-token
```

### IO Manager
Your assets can be stored in storage services. For now, we only support the AWS storage service. This can be configured like this.

```yaml
io-manager:
  aws:
    credentials:
      access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
      secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"
      region: "us-east-1"
    bucket_name: "discursus-io"
    key_prefix: "platform"
```

<br/><br/>


## Usage
Here are some examples of how to use this CLI tool:

### List assets
```bash
foo@bar:~$ damn ls
```

<img src="https://raw.githubusercontent.com/discursus-data/damn/release/0.2/resources/images/damn_ls.png" width="500px" />

List all assets for a specifc key group
```bash
foo@bar:~$ damn ls --prefix gdelt
```

<img src="https://raw.githubusercontent.com/discursus-data/damn/release/0.2/resources/images/damn_ls_prefix.png" width="400px" />

### Show details for a specific asset
```bash
foo@bar:~$ damn show gdelt/gdelt_gkg_articles
```

<img src="https://raw.githubusercontent.com/discursus-data/damn/release/0.2/resources/images/damn_ls_asset.png" width="550px" />

### Show metrics for a specific asset
```bash
foo@bar:~$ damn metrics gdelt/gdelt_gkg_articles
```

<img src="https://raw.githubusercontent.com/discursus-data/damn/release/0.2/resources/images/damn_metrics.png" width="550px" />

<br/><br/>


## Contribution
Contributions to the DAMN tool are always welcome. Whether it's feature requests, bug fixes, or new features, your contribution is appreciated.

<br/><br/>


## License
The DAMN tool is open-source software, licensed under MIT.