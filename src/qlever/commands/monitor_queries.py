from __future__ import annotations

import json
import re
import subprocess
import textwrap
import time

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import pretty_printed_query


def render_queries(monitor_queries_cmd, args) -> bool:
    try:
        monitored_queries = subprocess.check_output(
            monitor_queries_cmd, shell=True
        )
        monitored_queries_dict = json.loads(monitored_queries)
    except Exception as e:
        log.error(f"Failed to get active queries: {e}")
        return False

    if not monitored_queries_dict:
        log.info("No active queries on the server")
        return True

    queries = list(monitored_queries_dict.items())

    # Show the full SPARQL for a specific query.
    if args.query_id:
        # Try as a table index first, then as a server query ID.
        try:
            idx = int(args.query_id)
            if 1 <= idx <= len(queries):
                sparql_query = queries[idx - 1][1]
            else:
                sparql_query = None
        except ValueError:
            sparql_query = monitored_queries_dict.get(args.query_id)
        if not sparql_query:
            log.error("No active query found for the given ID")
            return False
        log.info(pretty_printed_query(sparql_query, False, args.system))
        return True

    # Table header.
    col_index = 3
    col_qid = max(len(qid) for qid, _ in queries)
    indent = " " * (2 + col_index + 2 + col_qid + 2)
    log.info(f"  {'#':<{col_index}}  {'Query ID':<{col_qid}}  SPARQL")

    for i, (qid, sparql) in enumerate(queries, 1):
        # Collapse whitespace for compact display.
        sparql_oneline = re.sub(r"\s+", " ", sparql).strip()
        if args.detailed:
            wrapped = textwrap.fill(
                sparql_oneline,
                width=100,
                initial_indent="",
                subsequent_indent=indent,
            )
            log.info(f"  {i:<{col_index}}  {qid:<{col_qid}}  {wrapped}")
        else:
            short_sparql = (
                sparql_oneline[:80] + "..."
                if len(sparql_oneline) > 80
                else sparql_oneline
            )
            log.info(
                f"  {i:<{col_index}}  {qid:<{col_qid}}  {short_sparql}"
            )

    return True


class MonitorQueriesCommand(QleverCommand):
    """
    Class for executing the `monitor-queries` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Show the currently active queries on the server"

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "server": ["access_token", "host_name", "port"],
            "runtime": ["system"],
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--sparql-endpoint",
            help="URL of the SPARQL endpoint, default is {host_name}:{port}",
        )
        subparser.add_argument(
            "--detailed",
            action="store_true",
            default=False,
            help="Show the full SPARQL text for each active query",
        )
        subparser.add_argument(
            "--query-id",
            help="Show the full SPARQL text for a specific query,"
            " either by its index (#) or server query ID",
        )
        subparser.add_argument(
            "--watch",
            action="store_true",
            default=False,
            help="Continuously refresh the list of active queries"
            " until interrupted with Ctrl-C",
        )
        subparser.add_argument(
            "--interval",
            type=float,
            default=2.0,
            help="Refresh interval in seconds when using --watch"
            " (default: 2.0)",
        )

    def execute(self, args) -> bool:
        sparql_endpoint = (
            args.sparql_endpoint
            if args.sparql_endpoint
            else f"{args.host_name}:{args.port}"
        )
        monitor_queries_cmd = (
            f'curl -s {sparql_endpoint} --data-urlencode "cmd=dump-active-queries" '
            f'--data-urlencode access-token="{args.access_token}"'
        )

        # Show them.
        self.show(monitor_queries_cmd, only_show=args.show)
        if args.show:
            return True

        if args.watch and args.interval < 0.5:
            log.error("--interval must be at least 0.5 seconds")
            return False
        if args.watch and args.query_id:
            log.error("--watch cannot be combined with --query-id")
            return False

        if args.watch:
            try:
                while True:
                    print("\033[H\033[2J", end="", flush=True)
                    render_queries(monitor_queries_cmd, args)
                    time.sleep(args.interval)
            except KeyboardInterrupt:
                return True

        return render_queries(monitor_queries_cmd, args)
