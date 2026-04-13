from __future__ import annotations

from qlever.commands.query import QueryCommand as QleverQueryCommand


class QueryCommand(QleverQueryCommand):
    """
    Send a SPARQL query to the Oxigraph server. Extends the base query
    command with Oxigraph's /query endpoint and supported result formats.
    This class is used as the base QueryCommand by all the other new engines.
    """

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "query",
            type=str,
            nargs="?",
            default="SELECT * WHERE { ?s ?p ?o } LIMIT 10",
            help="SPARQL query to send",
        )
        subparser.add_argument(
            "--predefined-query",
            type=str,
            choices=self.predefined_queries.keys(),
            help="Use a predefined query",
        )
        subparser.add_argument(
            "--sparql-endpoint", type=str, help="URL of the SPARQL endpoint"
        )
        subparser.add_argument(
            "--accept",
            type=str,
            choices=[
                "text/tab-separated-values",
                "text/csv",
                "application/sparql-results+json",
                "application/sparql-results+xml",
            ],
            default="text/tab-separated-values",
            help="Accept header for the SPARQL query",
        )
        subparser.add_argument(
            "--get",
            action="store_true",
            default=False,
            help="Use GET request instead of POST",
        )
        subparser.add_argument(
            "--no-time",
            action="store_true",
            default=False,
            help="Do not print the (end-to-end) time taken",
        )

    def execute(self, args) -> bool:
        # Oxigraph's SPARQL endpoint is at /query.
        if not args.sparql_endpoint:
            args.sparql_endpoint = f"{args.host_name}:{args.port}/query"
        # These QLever-specific options are not supported by Oxigraph.
        args.pin_to_cache = None
        args.access_token = None
        return super().execute(args)
