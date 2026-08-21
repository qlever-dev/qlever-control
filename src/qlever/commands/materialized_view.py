from __future__ import annotations

import json
import re
import shlex
import time
from pathlib import Path

from qlever.command import QleverCommand
from qlever.log import log
from qlever.qleverfile import Qleverfile
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
        return (
            "Create a materialized view from the given query, "
            "or load or delete an existing one"
        )

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
            nargs="?",
            default=None,
            help="SPARQL query from which to create the materialized view",
        )
        subparser.add_argument(
            "--sparql-endpoint",
            type=str,
            help="URL of the SPARQL endpoint (default: <host_name>:<port>)",
        )
        subparser.add_argument(
            "--load",
            action="store_true",
            default=False,
            help="Load an existing materialized view instead of creating one",
        )
        subparser.add_argument(
            "--delete",
            action="store_true",
            default=False,
            help="Delete an existing materialized view instead of "
            "creating one",
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

        if args.load and args.delete:
            log.error("Cannot use `--load` and `--delete` together")
            return False

        # With `--load` or `--delete`, no query must be given.
        if (args.load or args.delete) and args.view_query is not None:
            log.error(
                "A query must not be given together with "
                "`--load` or `--delete`"
            )
            return False

        # If `--delete` is set, delete an existing materialized view.
        if args.delete:
            url = (
                f"{sparql_endpoint}"
                f"?cmd=delete-materialized-view"
                f"&view-name={args.view_name}"
            )
            delete_cmd = (
                f"curl -s {shlex.quote(url)} "
                f"-H 'Authorization: Bearer {args.access_token}'"
            )
            self.show(delete_cmd, only_show=args.show)
            if args.show:
                return True
            try:
                result = run_command(delete_cmd, return_output=True)
            except Exception as e:
                log.error(f"Deleting the materialized view failed: {e}")
                return False
            try:
                result_json = json.loads(result)
            except json.JSONDecodeError:
                # An error response from the server is plain text, not JSON.
                log.error(
                    f"Deleting the materialized view failed: {result.strip()}"
                )
                return False
            view_name = result_json.get("materialized-view-deleted")
            log.info(f"Materialized view '{view_name}' deleted")
            return True

        # If `--load` is set, load an existing materialized view.
        if args.load:
            url = (
                f"{sparql_endpoint}"
                f"?cmd=load-materialized-view"
                f"&view-name={args.view_name}"
            )
            load_cmd = (
                f"curl -s {shlex.quote(url)} "
                f"-H 'Authorization: Bearer {args.access_token}'"
            )
            self.show(load_cmd, only_show=args.show)
            if args.show:
                return True
            try:
                result = run_command(load_cmd, return_output=True)
            except Exception as e:
                log.error(f"Loading the materialized view failed: {e}")
                return False
            try:
                result_json = json.loads(result)
            except json.JSONDecodeError:
                # An error response from the server is plain text, not JSON.
                log.error(
                    f"Loading the materialized view failed: {result.strip()}"
                )
                return False
            view_name = result_json.get("materialized-view-loaded")
            log.info(f"Materialized view '{view_name}' loaded")
            return True

        # If no query was given, try to take it from MATERIALIZED_VIEWS in
        # the Qleverfile (same key as used by `qlever index`). Read
        # directly instead of via `relevant_qleverfile_arguments`, which
        # would also add an (unwanted) `--materialized-views` option.
        if args.view_query is None:
            qleverfile_path = Path(getattr(args, "qleverfile", "Qleverfile"))
            if qleverfile_path.is_file():
                try:
                    qleverfile_config = Qleverfile.read(qleverfile_path)
                    materialized_views = json.loads(
                        qleverfile_config.get(
                            "index", "materialized_views", fallback="{}"
                        ).strip()
                        or "{}"
                    )
                except Exception as e:
                    log.error(
                        "Failed to read MATERIALIZED_VIEWS from "
                        f"`{qleverfile_path}`: {e}"
                    )
                    return False
                args.view_query = materialized_views.get(args.view_name)

        # A query is required when creating a materialized view.
        if args.view_query is None:
            log.error(
                f"No query given for materialized view '{args.view_name}', "
                "and none found for it in MATERIALIZED_VIEWS in the "
                "Qleverfile (use --load to load an existing one, or "
                "--delete to delete one)"
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
            log.info(
                "Creating the materialized view ... "
                "(this may take a while, depending on the complexity "
                "of the query and the size of the result)"
            )
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
