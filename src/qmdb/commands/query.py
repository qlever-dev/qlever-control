from __future__ import annotations

from qoxigraph.commands.query import QueryCommand as QoxigraphQueryCommand


class QueryCommand(QoxigraphQueryCommand):
    def execute(self, args) -> bool:
        if not args.sparql_endpoint:
            args.sparql_endpoint = f"localhost:{args.port}/sparql"
        super().execute(args)
