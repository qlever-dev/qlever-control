from __future__ import annotations

from qjena.commands.status import StatusCommand
from qoxigraph.commands.stop import StopCommand as QoxigraphStopCommand


class StopCommand(QoxigraphStopCommand):
    STATUS_COMMAND = StatusCommand()
    DEFAULT_REGEX = r".*fuseki-server.*--port\s%%PORT%%.*%%NAME%%.*"

    def description(self) -> str:
        return "Stop Jena server for a given dataset or port"

    def execute(self, args) -> bool:
        args.cmdline_regex = args.cmdline_regex.replace(
            "%%PORT%%", str(args.port)
        ).replace("%%NAME%%", args.name)
        return super().execute(args)
