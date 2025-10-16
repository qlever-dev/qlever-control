from __future__ import annotations

import shlex
import time
import traceback

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import run_command


class QueryCommand(QleverCommand):
    """
    Class for executing the `query` command.
    """

    def __init__(self):
        self.predefined_queries = {
            "all-predicates": (
                "SELECT (?p AS ?predicate) (COUNT(?p) AS ?count) "
                "WHERE { ?s ?p ?o } "
                "GROUP BY ?p ORDER BY DESC(?count)"
            ),
            "all-graphs": (
                "SELECT ?g (COUNT(?g) AS ?count) "
                "WHERE { GRAPH ?g { ?s ?p ?o } } "
                "GROUP BY ?g ORDER BY DESC(?count)"
            ),
            "ten-random-triples": (
                "SELECT * WHERE { ?s ?p ?o } ORDER BY RAND() LIMIT 10"
            ),
        }

    def description(self) -> str:
        return "Send a query to a SPARQL endpoint"

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {"server": ["host_name", "port", "access_token"]}

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
            "--pin-to-cache",
            action="store_true",
            default=False,
            help="Pin the query to the cache",
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
                "application/qlever-results+json",
                "application/octet-stream",
            ],
            default="text/tab-separated-values",
            help="Accept header for the SPARQL query",
        )
        subparser.add_argument(
            "--no-time",
            action="store_true",
            default=False,
            help="Do not print the (end-to-end) time taken",
        )

    def execute(self, args) -> bool:
        # Use a predefined query if requested.
        if args.predefined_query:
            args.query = self.predefined_queries[args.predefined_query]

        # When pinning to the cache, set `send=0` and request media type
        # `application/qlever-results+json` so that we get the result size.
        # Also, we need to provide the access token.
        if args.pin_to_cache:
            args.accept = "application/qlever-results+json"
            curl_cmd_additions = (
                f" --data pin-result=true --data send=0"
                f" --data access-token="
                f"{shlex.quote(args.access_token)}"
                f" | jq .resultsize | numfmt --grouping"
                f" | xargs -I {{}} printf"
                f' "Result pinned to cache,'
                f' number of rows: {{}}\\n"'
            )
        else:
            curl_cmd_additions = ""

        # Show what the command will do.
        sparql_endpoint = (
            args.sparql_endpoint
            if args.sparql_endpoint
            else f"{args.host_name}:{args.port}"
        )
        curl_cmd = (
            f"curl -s {sparql_endpoint}"
            f' -H "Accept: {args.accept}"'
            f" --data-urlencode query={shlex.quote(args.query)}"
            f"{curl_cmd_additions}"
        )
        self.show(curl_cmd, only_show=args.show)
        if args.show:
            return True

        # Launch query.
        try:
            start_time = time.time()
            run_command(curl_cmd, show_output=True)
            time_msecs = round(1000 * (time.time() - start_time))
            if not args.no_time and args.log_level != "NO_LOG":
                log.info("")
                log.info(
                    f"Query processing time (end-to-end):"
                    f" {time_msecs:,d} ms"
                )
        except Exception as e:
            if args.log_level == "DEBUG":
                traceback.print_exc()
            log.error(e)
            return False

        return True
