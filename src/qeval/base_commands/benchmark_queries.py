from __future__ import annotations

from abc import abstractmethod

from qeval.base_commands.index_stats import BaseIndexStatsCommand
from qlever.commands.benchmark_queries import (
    BenchmarkQueriesCommand as QleverBenchmarkQueriesCommand,
)


class BaseBenchmarkQueriesCommand(QleverBenchmarkQueriesCommand):
    """
    Run benchmark queries against the SPARQL endpoint of an engine.
    """

    @abstractmethod
    def index_stats_command(self) -> BaseIndexStatsCommand:
        """The command that reads the index time and size of the engine."""

    def can_clear_cache(self, args, engine: str | None) -> bool:
        # Clearing the cache before each query only works for QLever.
        return False
