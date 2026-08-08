from __future__ import annotations

from qlever import script_name
from qlever.commands.log import LogCommand as QleverLogCommand
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import run_command


class LogCommand(QleverLogCommand):
    """
    Show server logs for Oxigraph. For native execution, tails the log
    file as usual. For containers, uses `docker/podman logs` as it is
    not possible to redirect oxigraph logs to a log file.
    """

    def __init__(self):
        pass

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "data": ["name"],
            "runtime": [
                "system",
                "image",
                "server_container",
            ],
        }

    def execute(self, args) -> bool:
        if args.system not in Containerize.supported_systems():
            return super().execute(args)

        # Handle container logging using docker/podman logs command instead of tail
        # This is because we don't have <args.name>.server-log.txt for
        # containerized execution
        log_cmd = f"{args.system} logs "

        if not args.from_beginning:
            log_cmd += f"-n {args.tail_num_lines} "
        if not args.no_follow:
            log_cmd += "-f "

        log_cmd += args.server_container

        # Show the command line.
        self.show(log_cmd, only_show=args.show)
        if args.show:
            return True

        if not Containerize().is_running(args.system, args.server_container):
            log.error(f"No server container {args.server_container} found!\n")
            log.info(f"Are you sure you called `{script_name} start`?")
            return False

        try:
            run_command(log_cmd, show_output=True, show_stderr=True)
        except Exception as e:
            log.error(f"Cannot display container logs - {e}")
        return True
