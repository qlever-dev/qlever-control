from __future__ import annotations

import re
import subprocess

from qlever.command import QleverCommand
from qlever.commands.cache_stats import CacheStatsCommand
from qlever.log import log


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

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {"server": ["host_name", "port", "access_token"]}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--server-url",
            help="URL of the QLever server, default is {host_name}:{port}",
        )
        subparser.add_argument(
            "--complete",
            action="store_true",
            default=False,
            help="Clear the cache completely, including the pinned queries",
        )

    def execute(self, args) -> bool:
        # Construct command line and show it.
        clear_cache_cmd = "curl -s"
        if args.server_url:
            clear_cache_cmd += f" {args.server_url}"
        else:
            clear_cache_cmd += f" {args.host_name}:{args.port}"
        cmd_val = "clear-cache-complete" if args.complete else "clear-cache"
        clear_cache_cmd += f' --data-urlencode "cmd={cmd_val}"'
        if args.complete:
            clear_cache_cmd += (
                f" --data-urlencode access-token=" f'"{args.access_token}"'
            )
        self.show(clear_cache_cmd, only_show=args.show)
        if args.show:
            return True

        # Execute the command.
        try:
            clear_cache_cmd += ' -w " %{http_code}"'
            result = subprocess.run(
                clear_cache_cmd,
                shell=True,
                capture_output=True,
                text=True,
                check=True,
            ).stdout
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
