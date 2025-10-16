from __future__ import annotations

import json

from termcolor import colored

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import run_command


class SettingsCommand(QleverCommand):
    """
    Class for executing the `settings` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Show or set server settings (after `qlever start`)"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {"server": ["port", "host_name", "access_token"]}

    def additional_arguments(self, subparser) -> None:
        all_keys = [
            "always-multiply-unions",
            "cache-max-num-entries",
            "cache-max-size",
            "cache-max-size-single-entry",
            "cache-service-results",
            "default-query-timeout",
            "division-by-zero-is-undef",
            "enable-prefilter-on-index-scans",
            "group-by-disable-index-scan-optimizations",
            "group-by-hash-map-enabled",
            "lazy-index-scan-max-size-materialization",
            "lazy-index-scan-num-threads",
            "lazy-index-scan-queue-size",
            "lazy-result-max-cache-size",
            "query-planning-budget",
            "request-body-limit",
            "service-max-redirects",
            "service-max-value-rows",
            "sort-estimate-cancellation-factor",
            "spatial-join-prefilter-max-size",
            "spatial-join-max-num-threads",
            "syntax-test-mode",
            "throw-on-unbound-variables",
            "treat-default-graph-as-named-graph",
            "use-binsearch-transitive-path",
        ]
        subparser.add_argument(
            "runtime_parameter",
            nargs="?",
            help="Set the given runtime parameter (key=value)"
            "; if no argument is given, show all settings",
        ).completer = lambda **kwargs: [f"{key}=" for key in all_keys]
        subparser.add_argument(
            "--endpoint_url",
            type=str,
            help="An arbitrary endpoint URL "
            "(overriding the one in the Qleverfile)",
        )

    def execute(self, args) -> bool:
        # Get endpoint URL from command line or Qleverfile.
        if args.endpoint_url:
            endpoint_url = args.endpoint_url
        else:
            endpoint_url = f"http://{args.host_name}:{args.port}"

        # Construct the `curl` command for getting or setting.
        if args.runtime_parameter:
            try:
                parameter_key, parameter_value = args.runtime_parameter.split(
                    "="
                )
            except ValueError:
                log.error("Runtime parameter must be given as `key=value`")
                return False

            curl_cmd = (
                f"curl -s {endpoint_url}"
                f' --data-urlencode "{parameter_key}={parameter_value}"'
                f' --data-urlencode "access-token={args.access_token}"'
            )
        else:
            curl_cmd = (
                f"curl -s {endpoint_url}" f" --data-urlencode cmd=get-settings"
            )
            parameter_key, parameter_value = None, None
        self.show(curl_cmd, only_show=args.show)
        if args.show:
            return True

        # Execute the `curl` command. Note that the `get-settings` command
        # returns all settings in both scencarios (that is, also when setting a
        # parameter).
        try:
            settings_json = run_command(curl_cmd, return_output=True)
            settings_dict = json.loads(settings_json)
            if isinstance(settings_dict, list):
                settings_dict = settings_dict[0]
        except Exception as e:
            log.error(f"setting command failed: {e}")
            return False
        for key, value in settings_dict.items():
            print(
                colored(
                    f"{key:<45}: {value}",
                    "blue"
                    if parameter_key and key == parameter_key
                    else None,
                )
            )
        return True
