from __future__ import annotations

import json
import re
import shlex
import time

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import (
    run_command,
)


class MaterializedViewCommand(QleverCommand):
    """
    Class for executing the `materialized-view` command.
    """

    def __init__(self):
        self.materialized_view_name_regex = r"^[A-Za-z0-9-]+$"
        pass

    def description(self) -> str:
        return "Create a materialized view from the given query"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "data": ["name"],
            "server": ["host_name", "port", "access_token"],
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "view_name",
            type=str,
            help="Name of the materialized view",
        )
        subparser.add_argument(
            "view_query",
            type=str,
            help="SPARQL query from which to create the materialized view",
        )
        subparser.add_argument(
            "--sparql-endpoint",
            type=str,
            help="URL of the SPARQL endpoint (default: <host_name>:<port>)",
        )

    def execute(self, args) -> bool:
        # SPARQL endpoint to use.
        sparql_endpoint = (
            args.sparql_endpoint
            if args.sparql_endpoint is not None
            else f"{args.host_name}:{args.port}"
        )

        # Check that the name of the materialized view is valid.
        if not re.match(self.materialized_view_name_regex, args.view_name):
            log.error(
                f"The name for the materialized view must match "
                f"the regex {self.materialized_view_name_regex}"
            )
            return False

        # Command for building the materialized view.
        url = (
            f"{sparql_endpoint}"
            f"?cmd=write-materialized-view"
            f"&view-name={args.view_name}"
        )
        materialized_view_cmd = (
            f"curl -s {shlex.quote(url)} "
            f"-H 'Authorization: Bearer {args.access_token}' "
            f"-H 'Content-type: application/sparql-query' "
            f"-d {shlex.quote(args.view_query)}"
        )
        self.show(materialized_view_cmd, only_show=args.show)
        if args.show:
            return True

        # Run the command (and time it).
        time_start = time.monotonic()
        try:
            log.info("Creating the materialized view ... "
                     "(this may take a while, depending on the complexity "
                     "of the query and the size of the result)")
            log.info("")
            result = run_command(materialized_view_cmd, return_output=True)
        except Exception as e:
            log.error(f"Creating the materialized view failed: {e}")
            return False
        time_end = time.monotonic()
        duration_seconds = round(time_end - time_start)

        # Try to parse the result (should be JSON).
        try:
            result_json = json.loads(result)
            view_name = result_json.get("materialized-view-written")
            log.info(
                f"Materialized view '{view_name}' created successfully "
                f"in {duration_seconds:,} seconds"
            )
        except Exception as e:
            log.error(f'Failed to parse JSON from "{result}": {e}')

        return True
