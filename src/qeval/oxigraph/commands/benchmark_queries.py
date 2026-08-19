from __future__ import annotations

from qlever.commands.benchmark_queries import (
    BenchmarkQueriesCommand as QleverBenchmarkQueriesCommand,
)


class BenchmarkQueriesCommand(QleverBenchmarkQueriesCommand):
    """
    Run benchmark queries against the Oxigraph SPARQL endpoint.
    Overrides the default endpoint to use Oxigraph's /query path.
    """

    def execute(self, args) -> bool:
        if not args.sparql_endpoint:
            args.sparql_endpoint = f"{args.host_name}:{args.port}/query"
        return super().execute(args)
