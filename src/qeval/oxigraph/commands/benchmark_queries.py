from __future__ import annotations

from qeval.base_commands.benchmark_queries import BaseBenchmarkQueriesCommand
from qeval.oxigraph.commands.index_stats import IndexStatsCommand


class BenchmarkQueriesCommand(BaseBenchmarkQueriesCommand):
    """Run benchmark queries against the Oxigraph SPARQL endpoint."""

    def index_stats_command(self) -> IndexStatsCommand:
        return IndexStatsCommand()
