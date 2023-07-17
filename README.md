    ████████▄     ▄████████   ▄▄▄▄███▄▄▄▄   ███▄▄▄▄   
    ███   ▀███   ███    ███ ▄██▀▀▀███▀▀▀██▄ ███▀▀▀██▄ 
    ███    ███   ███    ███ ███   ███   ███ ███   ███ 
    ███    ███   ███    ███ ███   ███   ███ ███   ███ 
    ███    ███ ▀███████████ ███   ███   ███ ███   ███ 
    ███    ███   ███    ███ ███   ███   ███ ███   ███ 
    ███   ▄███   ███    ███ ███   ███   ███ ███   ███ 
    ████████▀    ███    █▀   ▀█   ███   █▀   ▀█   █▀                                                 


<br/><br/>
<p align="center">
    <a href="https://github.com/discursus-data/damn/releases">
        <img src="https://img.shields.io/github/release/discursus-data/damn" alt="Latest release" />
    </a>
    <a href="https://github.com/discursus-data/damn/network/members">
        <img src="https://img.shields.io/github/forks/discursus-data/damn" alt="Forks" />
    </a>
    <a href="https://github.com/discursus-data/damn/stargazers">
        <img src="https://img.shields.io/github/stars/discursus-data/damn" alt="Stars" />
    </a>
    <a href="https://github.com/discursus-data/damn/issues">
        <img src="https://img.shields.io/github/issues/discursus-data/damn" alt="Open issues" />
    </a>
    <a href="https://github.com/discursus-data/damn/contributors/">
        <img src="https://img.shields.io/github/contributors/discursus-data/damn" alt="Contributors" />
    </a>
</p>
<br/><br/>

# Data Asset Metrics Navigator
The DAMN tool extracts and reports metrics about your data assets.

It allows you to inspect your assets, lineage, and all sorts of metrics around materialization, usage, physical space usage and query performance. The objective of the DAMN tool is to give you a convenient command-line tool to track and report on the data assets you're working on.

## Installation
To install the DAMN tool, run the following command:

```bash
pip install damn-tool
```

<br/><br/>


## Connectors
The DAMN tool leverages various connectors to interact with different service providers.


### Configurations
Configuring these connectors is done via a YAML file located at `~/.damn/connectors.yml`.

[*See example configuration file here*](connectors.yml.REPLACE)

The configuration file uses the following structure:

```yaml
connector_type:
  profile_name:
    param1: value1
    param2: value2
```

- connector_type: The name of the connector (e.g., orchestrator, io-manager, data-warehouse, etc.).
- profile_name: The name of the profile for the connector. You can have multiple profiles per connector (e.g., prod, dev, test, etc.).
- param1, param2, etc.: The parameters needed for each connector. The required parameters will depend on the specific connector. For example, a Dagster connector might require endpoint and api_token.


### Connector types
#### Orchestrator
This is the default connector required by the DAMN tool. For now, we only support Dagster as the service provider for this connector. Here's an example configuration for an orchestrator connector with a dagster profiles:

```yaml
orchestrator:
  dagster:
    endpoint: https://your-dagster-instance.com/prod/graphql
    api_token: your-api-token
```

#### IO Manager
Your assets can be stored in storage services. For now, we only support the AWS storage service. This can be configured like this.

```yaml
io-manager:
  aws:
    credentials:
      access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
      secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"
      region: "us-east-1"
    bucket_name: "bucket-name"
    key_prefix: "asset-prefix"
```

### Switching Between Profiles
The active profile for each connector can be changed by specifying the profile when running damn commands. By default, damn will use the first profile configured for each connector.

Example usage:

```bash
damn ls --profile dagster
```

<br/><br/>


## Usage
Here are some examples of how to use this CLI tool:

### Output option
Note that all commands support an `output` option which allows flexibility in how the DAMN tool might be used:
- `terminal`: By default, the output of commands will be printed to the terminal
- `json`: You can also have the output as a `json` object, which is more useful if you're to use DAMN in a programmatic way.
- `copy`: You can also copy the output to your clipboard, which is useful if you want to share an asset's metrics in a PR for example.

### List assets
```bash
foo@bar:~$ damn ls
```

```
- airbyte/protest_groupings
- data_warehouse/movements_dim
- data_warehouse/observations_fct
- gdelt/gdelt_gkg_articles
- gdelt/gdelt_mention_summaries
- hex/hex_main_dashboard_refresh
- semantic_definitions
```

List all assets for a specifc key group
```bash
foo@bar:~$ damn ls --prefix gdelt
```

```
- gdelt/gdelt_article_summaries
- gdelt/gdelt_articles_enhanced
- gdelt/gdelt_events
- gdelt/gdelt_gkg_articles
- gdelt/gdelt_mention_summaries
- gdelt/gdelt_mentions
- gdelt/gdelt_mentions_enhanced
```

### Show details for a specific asset
```bash
foo@bar:~$ damn show gdelt/gdelt_gkg_articles
```

```
Asset attributes:
- Key: gdelt/gdelt_gkg_articles
- Description: List of gkg articles mined on GDELT
- Compute kind: None
- Is partitioned: True
- Auto-materialization policy: EAGER
- Freshess policy (maximum lag minutes): Not available
- Freshess policy (cron schedule): Not available

Upstream assets:
- None

Downstream assets:
- data_warehouse/staging/stg__gdelt__articles
- gdelt/gdelt_articles_enhanced

Latest materialization's metadata entries:
- Last materialization timestamp: 1689093222451
- s3_path: s3://discursus-io/sources/gdelt/20230711/20230711161500.articles.csv
- rows: 13
- min_gdelt_gkg_article_id: 20230711161500-1968
- max_gdelt_gkg_article_id: 20230711161500-T589
- path: platform/gdelt/gdelt_gkg_articles/20230711161500
- uri: s3://discursus-io/platform/gdelt/gdelt_gkg_articles
```

### Show metrics for a specific asset
```bash
foo@bar:~$ damn metrics gdelt/gdelt_gkg_articles
```

```
Latest orchestrator materialization metrics:
- Latest run ID: ee9d7c67-cf31-411b-96e8-038db0252ef1
- Status: SUCCESS
- Start time: 2023-07-13 09:33:18
- End time: 2023-07-13 09:33:20
- Elapsed time: 0:00:02.623300


Orchestrator partitions:
- Number of partitions: 4368
- Materialized partitions: 4368
- Failed partitions: 0


IO Manager:
- Files: 4381
- File(s) size: 63.72 MB
- Last modified: 2023-07-13 13:33:21+00:00
```

<br/><br/>


## Contribution
Contributions to the DAMN tool are always welcome. Whether it's feature requests, bug fixes, or new features, your contribution is appreciated.

<br/><br/>


## License
The DAMN tool is open-source software, licensed under MIT.