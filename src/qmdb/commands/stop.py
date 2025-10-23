from __future__ import annotations

from qmdb.commands.status import StatusCommand
from qoxigraph.commands.stop import StopCommand as QoxigraphStopCommand


class StopCommand(QoxigraphStopCommand):
    STATUS_COMMAND = StatusCommand()
    DEFAULT_REGEX = r"mdb-server.*--port\s%%PORT%%.*"

    def description(self) -> str:
        return "Stop MillenniumDB server for a given dataset or port"

    def execute(self, args) -> bool:
        args.cmdline_regex = args.cmdline_regex.replace(
            "%%PORT%%", str(args.port)
        )
        return super().execute(args)
