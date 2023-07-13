from line_profiler import LineProfiler
from damn_tool.metrics import get_dagster_metrics

def do_profile():
    lp = LineProfiler()
    lp.add_function(get_dagster_metrics)
    lp.runcall(get_dagster_metrics, "gdelt/gdelt_gkg_articles", "prod")
    lp.print_stats()

do_profile()