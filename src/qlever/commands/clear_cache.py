from __future__ import annotations

import re

from qlever.command import QleverCommand
from qlever.commands.cache_stats import CacheStatsCommand
from qlever.log import log
from qlever.util import run_command


class ClearCacheCommand(QleverCommand):
    """
    Class for executing the `clear-cache` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Clear the query processing cache"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {"server": ["host_name", "port", "access_token"]}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--sparql-endpoint",
            help="URL of the QLever server, default is {host_name}:{port}",
        )
        subparser.add_argument(
            "--complete",
            action="store_true",
            default=False,
            help="Clear the cache completely, including the pinned queries",
        )

    def execute(self, args) -> bool:
        # Determine SPARQL endpoint.
        sparql_endpoint = (
            args.sparql_endpoint
            if args.sparql_endpoint
            else (f"{args.host_name}:{args.port}")
        )

        # Construct command line and show it.
        clear_cache_cmd = f"curl -s {sparql_endpoint} -d cmd=clear-cache"
        if args.complete:
            clear_cache_cmd += (
                f"-complete"
                f' --data-urlencode access-token="{args.access_token}"'
            )
        self.show(clear_cache_cmd, only_show=args.show)
        if args.show:
            return True

        # Execute the command.
        try:
            clear_cache_cmd += ' -w " %{http_code}"'
            result = run_command(clear_cache_cmd, return_output=True)
            match = re.match(r"^(.*) (\d+)$", result, re.DOTALL)
            if not match:
                raise Exception(f"Unexpected output:\n{result}")
            error_message = match.group(1).strip()
            status_code = match.group(2)
            if status_code != "200":
                raise Exception(error_message)
            message = "Cache cleared successfully"
            if args.complete:
                message += " (pinned and unpinned queries)"
            else:
                message += " (only unpinned queries)"
            log.info(message)
        except Exception as e:
            log.error(e)
            return False

        # Show cache stats.
        log.info("")
        args.detailed = False
        if not CacheStatsCommand().execute(args):
            log.error(
                "Clearing the cache was successful, but showing the "
                "cache stats failed {e}"
            )
        return True
