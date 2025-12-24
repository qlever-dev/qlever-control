from __future__ import annotations

import json

from termcolor import colored

from qlever.command import QleverCommand
from qlever.log import log
from qlever.qleverfile import Qleverfile
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

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {"server": ["port", "host_name", "access_token"]}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "runtime_parameters",
            nargs="*",
            help="Space-separated list of runtime parameters to set "
            "in the form `key=value`; afterwards shows all settings, "
            "with the changed ones highlighted",
        ).completer = lambda **kwargs: [
            f"{key}=" for key in Qleverfile.SERVER_RUNTIME_PARAMETERS
        ]
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

        # Construct the `curl` commands for setting and getting.
        curl_cmds_setting = []
        keys_set = set()
        if args.runtime_parameters:
            for key_value_pair in args.runtime_parameters:
                try:
                    key, value = key_value_pair.split("=")
                except ValueError:
                    log.error("Runtime parameter must be given as `key=value`")
                    return False
                curl_cmds_setting.append(
                    f"curl -s {endpoint_url} -w %{{http_code}}"
                    f' --data-urlencode "{key}={value}"'
                    f' --data-urlencode "access-token={args.access_token}"'
                )
                keys_set.add(key)
        curl_cmd_getting = (
            f"curl -s {endpoint_url} -w %{{http_code}}"
            f" --data-urlencode cmd=get-settings"
        )
        self.show(
            "\n".join(curl_cmds_setting + [curl_cmd_getting]),
            only_show=args.show,
        )
        if args.show:
            return True

        # Execute the `curl` commands for setting the key-value pairs if any.
        for curl_cmd in curl_cmds_setting:
            try:
                curl_result = run_command(curl_cmd, return_output=True)
                body, http_code = curl_result[:-3], curl_result[-3:]
                if http_code != "200":
                    raise Exception(body)
            except Exception as e:
                log.error(
                    f"curl command for setting key-value pair failed: {e}"
                )
                return False

        # Execute the `curl` commands for getting the settings.
        try:
            curl_result = run_command(curl_cmd_getting, return_output=True)
            body, http_code = curl_result[:-3], curl_result[-3:]
            if http_code != "200":
                raise Exception(body)
            settings_dict = json.loads(body)
            if isinstance(settings_dict, list):
                settings_dict = settings_dict[0]
        except Exception as e:
            log.error(f"curl command for getting settings failed: {e}")
            return False
        for key, value in settings_dict.items():
            print(
                colored(
                    f"{key:<45}: {value}",
                    "blue" if key in keys_set else None,
                )
            )

        # That's it.
        return True
