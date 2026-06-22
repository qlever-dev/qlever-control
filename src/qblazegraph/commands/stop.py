from __future__ import annotations

from qblazegraph.commands.status import StatusCommand
from qoxigraph.commands.stop import StopCommand as QoxigraphStopCommand


class StopCommand(QoxigraphStopCommand):
    STATUS_COMMAND = StatusCommand()
    DEFAULT_REGEX = "java\\s+-server.*=%%PORT%%.*blazegraph.jar"

    def description(self) -> str:
        return "Stop Blazegraph server for a given dataset or port"

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        # Add port in the server section to the arguments
        args = super().relevant_qleverfile_arguments()
        args.setdefault("server", []).append("port")
        return args

    def execute(self, args) -> bool:
        args.cmdline_regex = args.cmdline_regex.replace(
            "%%PORT%%", str(args.port)
        )
        return super().execute(args)
