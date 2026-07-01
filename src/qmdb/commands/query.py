from __future__ import annotations

import shlex
import time
import traceback

from qlever.log import log
from qlever.util import run_command
from qoxigraph.commands.query import QueryCommand as QoxigraphQueryCommand


class QueryCommand(QoxigraphQueryCommand):
    """
    Send a SPARQL query to the MillenniumDB server. Extends the base query
    command with MillenniumDB's /sparql endpoint.
    """

    def execute(self, args, called_from_conformance_test: bool = False) -> bool:
        if not args.sparql_endpoint:
            args.sparql_endpoint = f"{args.host_name}:{args.port}/sparql"
        if not called_from_conformance_test:
            return super().execute(
                args, called_from_conformance_test=called_from_conformance_test
            )

        if args.predefined_query:
            args.query = self.predefined_queries[args.predefined_query]

        content_type = getattr(args, "content_type", None)
        use_raw_body = content_type in (
            "application/sparql-query",
            "application/sparql-update",
        )
        curl_cmd = (
            f"curl -s {args.sparql_endpoint}"
            f' -H "Accept: {args.accept}"'
        )
        if use_raw_body:
            curl_cmd += (
                f' -H "Content-Type: {content_type}"'
                f" --data-binary {shlex.quote(args.query)}"
            )
        else:
            curl_cmd += (
                ' -H "Content-Type: application/x-www-form-urlencoded"'
                f" --data-urlencode query={shlex.quote(args.query)}"
            )
        curl_cmd += " -w '\\nHTTP_STATUS:%{http_code}'"
        max_time = getattr(args, "curl_max_time", None)
        if max_time:
            curl_cmd += f" --max-time {shlex.quote(str(max_time))}"

        self.show(curl_cmd, only_show=args.show)
        if args.show:
            return True

        try:
            start_time = time.time()
            self.query_output = run_command(curl_cmd, return_output=True)
            time_msecs = round(1000 * (time.time() - start_time))
            if not args.no_time and args.log_level != "NO_LOG":
                log.info("")
                log.info(
                    "Query processing time (end-to-end): "
                    f"{time_msecs:,d} ms"
                )
        except Exception as e:
            if args.log_level == "DEBUG":
                traceback.print_exc()
            log.error(e)
            return False

        return True
