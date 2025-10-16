from __future__ import annotations

import json
import re
import subprocess

from qlever.command import QleverCommand
from qlever.log import log


class CacheStatsCommand(QleverCommand):
    """
    Class for executing the `warmup` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Show how much of the cache is currently being used"

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {"server": ["host_name", "port"]}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--server-url",
            help="URL of the QLever server, default is {host_name}:{port}",
        )
        subparser.add_argument(
            "--detailed",
            action="store_true",
            default=False,
            help="Show detailed statistics and settings",
        )

    def execute(self, args) -> bool:
        # Construct the two curl commands.
        server_url = (
            args.server_url
            if args.server_url
            else f"{args.host_name}:{args.port}"
        )
        cache_stats_cmd = (
            f'curl -s {server_url} --data-urlencode "cmd=cache-stats"'
        )
        cache_settings_cmd = (
            f'curl -s {server_url} --data-urlencode "cmd=get-settings"'
        )

        # Show them.
        self.show(
            "\n".join([cache_stats_cmd, cache_settings_cmd]),
            only_show=args.show,
        )
        if args.show:
            return True

        # Execute them.
        try:
            cache_stats = subprocess.check_output(cache_stats_cmd, shell=True)
            cache_settings = subprocess.check_output(
                cache_settings_cmd, shell=True
            )
            cache_stats_dict = json.loads(cache_stats)
            cache_settings_dict = json.loads(cache_settings)
            if isinstance(cache_settings_dict, list):
                cache_settings_dict = cache_settings_dict[0]
        except Exception as e:
            log.error(f"Failed to get cache stats and settings: {e}")
            return False

        # Brief version.
        if not args.detailed:
            cache_size = cache_settings_dict["cache-max-size"]
            if not cache_size.endswith(" GB"):
                log.error(
                    f"Cache size {cache_size} is not in GB, "
                    f"QLever should return bytes instead"
                )
                return False
            else:
                cache_size = float(cache_size[:-3])
            pinned_size = cache_stats_dict["cache-size-pinned"] / 1e9
            non_pinned_size = cache_stats_dict["cache-size-unpinned"] / 1e9
            cached_size = pinned_size + non_pinned_size
            free_size = cache_size - cached_size
            if cached_size == 0:
                log.info(f"Cache is empty, all {cache_size:.1f} GB available")
            else:
                log.info(
                    f"Pinned queries     : "
                    f"{pinned_size:5.1f} GB of {cache_size:5.1f} GB"
                    f"  [{pinned_size / cache_size:5.1%}]"
                )
                log.info(
                    f"Non-pinned queries : "
                    f"{non_pinned_size:5.1f} GB of {cache_size:5.1f} GB"
                    f"  [{non_pinned_size / cache_size:5.1%}]"
                )
                log.info(
                    f"FREE               : "
                    f"{free_size:5.1f} GB of {cache_size:5.1f} GB"
                    f"  [{1 - cached_size / cache_size:5.1%}]"
                )
            return True

        # Complete version.
        def show_dict_as_table(key_value_pairs):
            max_key_len = max([len(key) for key, _ in key_value_pairs])
            for key, value in key_value_pairs:
                if isinstance(value, int) or re.match(r"^\d+$", value):
                    value = "{:,}".format(int(value))
                if re.match(r"^\d+\.\d+$", value):
                    value = "{:.2f}".format(float(value))
                log.info(f"{key.ljust(max_key_len)} : {value}")

        show_dict_as_table(cache_stats_dict.items())
        log.info("")
        show_dict_as_table(cache_settings_dict.items())

        return True
