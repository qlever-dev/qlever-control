from __future__ import annotations

from qoxigraph.commands.query import QueryCommand as QoxigraphQueryCommand


class QueryCommand(QoxigraphQueryCommand):
    """
    Send a SPARQL query to the Virtuoso server. Extends the base query
    command with Virtuoso's /sparql endpoint
    """

    def execute(self, args) -> bool:
        if not args.sparql_endpoint:
            args.sparql_endpoint = f"{args.host_name}:{args.port}/sparql"
        return super().execute(args)
