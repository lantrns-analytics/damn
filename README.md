
████████▄     ▄████████   ▄▄▄▄███▄▄▄▄   ███▄▄▄▄   
███   ▀███   ███    ███ ▄██▀▀▀███▀▀▀██▄ ███▀▀▀██▄ 
███    ███   ███    ███ ███   ███   ███ ███   ███ 
███    ███   ███    ███ ███   ███   ███ ███   ███ 
███    ███ ▀███████████ ███   ███   ███ ███   ███ 
███    ███   ███    ███ ███   ███   ███ ███   ███ 
███   ▄███   ███    ███ ███   ███   ███ ███   ███ 
████████▀    ███    █▀   ▀█   ███   █▀   ▀█   █▀  
                                                  


# Data Asset Manager Navigator

The DAMN tool is a command-line interface (CLI) tool to navigate and understand the data assets of your data platform, specifically tailored for Dagster.

This tool allows you to inspect your assets, understand their relationships, and get details such as description, compute kind, partitioning, auto-materialization policy, and freshness policy. The objective of the DAMN tool is to give you a convenient command-line tool for managing and understanding your data assets.

## Installation

To install the DAMN tool, run the following command:

```bash
pip install damn-tool
```

## Usage
To use the DAMN tool, you will first need to set the GRAPHQL_URL and GRAPHQL_TOKEN environment variables to point to your Dagster GraphQL API.

Here are some examples of how to use this CLI tool:

### List all assets
```bash
damn ls
```

### Get details for a specific asset
```bash
damn ls my_asset
```


## Contribution

Contributions to the DAMN tool are always welcome. Whether it's feature requests, bug fixes, or new features, your contribution is appreciated. Please make sure to read the contributing guide before making a pull request.


## License

The DAMN tool is open-source software, licensed under MIT.