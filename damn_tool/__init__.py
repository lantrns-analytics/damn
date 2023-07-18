import click

from .adapters.data_warehouses.snowflake import SnowflakeAdapter
from .utils.helpers import (
    load_config, 
    package_command_output, 
    print_packaged_command_output, 
    run_and_capture
)

from .ls import ls
from .show import show
from .metrics import metrics

@click.group()
@click.option('--data-warehouse', default=None, help='Data warehouse service provider to use')
@click.pass_context
def cli(ctx, data_warehouse):
    connector_type, data_warehouse_config = load_config('data-warehouse', data_warehouse)

    ctx.ensure_object(dict)  # Ensure the context is a dictionary
    ctx.obj['DATA_WAREHOUSE'] = connector_type
    ctx.obj['SNOWFLAKE_ADAPTER'] = SnowflakeAdapter(data_warehouse_config)

cli.add_command(ls)
cli.add_command(show)
cli.add_command(metrics)