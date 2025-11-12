from __future__ import annotations

from qjena.commands.status import StatusCommand
from qoxigraph.commands.stop import StopCommand as QoxigraphStopCommand


class StopCommand(QoxigraphStopCommand):
    STATUS_COMMAND = StatusCommand()
    DEFAULT_REGEX = r".*-Dgraphdb\.home=%%NAME%%\S*.*GraphDBServer$"

    def relevant_qleverfile_arguments(self) -> dict[str : list[str]]:
        return {
            "data": ["name"],
            "runtime": ["system", "server_container"],
        }

    def description(self) -> str:
        return "Stop the GraphDB server"

    def execute(self, args) -> bool:
        args.cmdline_regex = args.cmdline_regex.replace("%%NAME%%", args.name)
        return super().execute(args)
