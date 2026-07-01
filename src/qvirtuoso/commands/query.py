from __future__ import annotations

from qoxigraph.commands.query import QueryCommand as QoxigraphQueryCommand


class QueryCommand(QoxigraphQueryCommand):
    """
    Send a SPARQL query to the Virtuoso server. Extends the base query
    command with Virtuoso's /sparql endpoint
    """

    def execute(self, args, called_from_conformance_test: bool = False) -> bool:
        if not args.sparql_endpoint:
            args.sparql_endpoint = f"{args.host_name}:{args.port}/sparql"
        if called_from_conformance_test and not hasattr(args, "curl_max_time"):
            args.curl_max_time = 35
        if called_from_conformance_test:
            default_graph = getattr(args, "default_graph_uri", None)
            if default_graph and "define input:default-graph-uri" not in args.query.lower():
                args.query = (
                    f"DEFINE input:default-graph-uri <{default_graph}>\n"
                    f"{args.query}"
                )
        return super().execute(
            args, called_from_conformance_test=called_from_conformance_test
        )
