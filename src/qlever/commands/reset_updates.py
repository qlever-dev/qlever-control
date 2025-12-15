from __future__ import annotations

import re
import subprocess

from qlever.command import QleverCommand
from qlever.log import log


class ResetUpdatesCommand(QleverCommand):
    """
    Class for executing the `reset-updates` command.
    """


    def description(self) -> str:
        return "Reset the updates on the server"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {"server": ["host_name", "port", "access_token"]}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--server-url",
            help="URL of the QLever server, default is {host_name}:{port}",
        )

    def execute(self, args) -> bool:
        reset_cmd = "curl -s"
        if args.server_url:
            reset_cmd += f" {args.server_url}"
        else:
            reset_cmd += f" {args.host_name}:{args.port}"
        reset_cmd += f' --data-urlencode "cmd=clear-delta-triples" --data-urlencode "access-token={args.access_token}"'
        self.show(reset_cmd, only_show=args.show)
        if args.show:
            return True

        try:
            reset_cmd += ' -w " %{http_code}"'
            result = subprocess.run(
                reset_cmd,
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
            message = "Updates reset successfully"
            log.info(message)
            return True
        except Exception as e:
            log.error(e)
            return False
