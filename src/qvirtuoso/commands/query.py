from __future__ import annotations

from qoxigraph.commands.query import QueryCommand as QoxigraphQueryCommand


class QueryCommand(QoxigraphQueryCommand):
    """
    Send a SPARQL query to the Virtuoso server. Defaults the endpoint
    to <host_name>:<port>/sparql if not explicitly provided.
    """

    def execute(self, args) -> bool:
        if not args.sparql_endpoint:
            args.sparql_endpoint = f"{args.host_name}:{args.port}/sparql"
        return super().execute(args)
