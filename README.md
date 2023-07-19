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
  service_provider:
    param1: value1
    param2: value2
```

- connector_type: The name of the connector (e.g., orchestrator, io-manager, data-warehouse, etc.).
- service_provider: The name of the service provider for the connector. You can have multiple providers per connector.
- param1, param2, etc.: The parameters needed for each connector. The required parameters will depend on the specific connector. For example, a Dagster connector might require endpoint and api_token.


### Connector types
#### Orchestrator
This is the default connector required by the DAMN tool. For now, we only support Dagster as the service provider for this connector. Here's an example configuration for an orchestrator connector with a dagster profile:

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

#### Data Warehouses
Your assets can be materialized to a data warehouse. For now, we only support Snowflake. This can be configured like this.

```yaml
data-warehouse:
  snowflake:
    account: ab1234.us-east-1
    user: username
    password: "{{ env('SNOWFLAKE_PASSWORD') }}"
    role: my-role
    database: my-database
    warehouse: my-warehouse
    schema: analytics
```

### Switching Between Service Providers
The active service provider for each connector can be changed by specifying the service provider when running DAMN commands. By default, DAMN will use the first service provider configured for each connector.

Example usage:

```bash
damn ls --orchestrator dagster --io-manager aws --data-warehouse snowflake
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
foo@bar:~$ damn show gdelt/data_warehouse/integration/int__events_actors
```

```
From orchestrator:
 - description: dbt model int__events_actors
 - computeKind: dbt
 - policyType: LAZY
 - maximumLagMinutes: 360.0
 - cronSchedule: None
 - isPartitioned: False
- dependedByKeys:
   - data_warehouse
   - events_actors_bridge
- dependencyKeys:
   - data_warehouse
   - integration
   - int__events_observations
   - data_warehouse
   - integration
   - int__actors
- metadataEntries:
  - Execution Duration: 4.183706
From data warehouse:
 - table_schema: analytics_integration
 - table_type: base table
 - created: 2023-07-05T08:36:40.935000-07:00
 - last_altered: 2023-07-19T09:56:36.410000-07:00

```

### Show metrics for a specific asset
```bash
foo@bar:~$ damn metrics gdelt/gdelt_gkg_articles
```

```
From orchestrator:
 - run_id: 03466ceb-1c51-43ab-9384-33b6472c3f24
 - status: SUCCESS
 - start_time: 2023-07-19 14:19:00
 - end_time: 2023-07-19 14:19:02
 - elapsed_time: 0:00:02.563292
 - num_partitions: 4963
 - num_materialized: 4963
 - num_failed: 0
From IO manager:
 - files: 4976
 - size: 76.25 MB
 - last_modified: 2023-07-19T18:19:03+00:00
From data warehouse:
 - row_count: None
 - bytes: N/A
```

<br/><br/>


## Contribution
Contributions to the DAMN tool are always welcome. Whether it's feature requests, bug fixes, or new features, your contribution is appreciated.

<br/><br/>


## License
The DAMN tool is open-source software, licensed under MIT.