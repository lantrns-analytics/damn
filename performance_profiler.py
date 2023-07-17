from line_profiler import LineProfiler
from damn_tool.metrics import get_orchestrator_metrics

def do_profile():
    lp = LineProfiler()
    lp.add_function(get_orchestrator_metrics)
    lp.runcall(get_orchestrator_metrics, "gdelt/gdelt_gkg_articles", "prod")
    lp.print_stats()

do_profile()