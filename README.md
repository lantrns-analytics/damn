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

## Connectors
The DAMN tool depends on connectors to your platform's services, such as orchestrators, data warehouses, file storage, API, etc.

### Dagster
This is for the moment the default and only connector supported by the DAMN tool. You'll need to set the following 2 environment variables to make it work:
- DAGSTER_GRAPHQL_URL, which points to your Dagster GraphQL instance
- DAGSTER_CLOUD_API_TOKEN, which is to authenticate you with your Dagster GraphQL instance


## Usage
Here are some examples of how to use this CLI tool:

### List all assets
```bash
foo@bar:~$ damn ls
```

Output
```bash
- airbyte/protest_groupings
- data_warehouse/actors_dim
- data_warehouse/events_actors_bridge
- data_warehouse/events_fct
- data_warehouse/events_observations_bridge
- ...
```

### List all assets within a specific group
```bash
foo@bar:~$ damn ls --prefix gdelt
```

Output
```bash
- gdelt/gdelt_article_entity_extraction
- gdelt/gdelt_article_summaries
- gdelt/gdelt_articles_enhanced
- gdelt/gdelt_enhanced_articles
- gdelt/gdelt_events
- ...
```

### Get details for a specific asset
```bash
foo@bar:~$ damn ls gdelt/gdelt_gkg_articles
```

Output
```bash
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

Latest materialization metadata entries:
- Last materialization timestamp: 1688659402892
- s3_path: s3://discursus-io/sources/gdelt/20230706/20230706154500.articles.csv
- rows: 11
- min_gdelt_gkg_article_id: 20230706154500-1138
- max_gdelt_gkg_article_id: 20230706154500-431
- path: platform/gdelt/gdelt_gkg_articles/20230706154500
- uri: s3://discursus-io/platform/gdelt/gdelt_gkg_articles
```


## Contribution
Contributions to the DAMN tool are always welcome. Whether it's feature requests, bug fixes, or new features, your contribution is appreciated.


## License
The DAMN tool is open-source software, licensed under MIT.