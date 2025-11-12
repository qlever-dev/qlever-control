from __future__ import annotations

from qoxigraph.commands.stop import StopCommand as QoxigraphStopCommand
from qvirtuoso.commands.status import StatusCommand


class StopCommand(QoxigraphStopCommand):
    STATUS_COMMAND = StatusCommand()
    DEFAULT_REGEX = r"virtuoso-t.*-c\s%%NAME%%.*"

    def description(self) -> str:
        return "Stop Virtuoso server for a given dataset or port"

    def execute(self, args) -> bool:
        args.cmdline_regex = args.cmdline_regex.replace("%%NAME%%", args.name)
        return super().execute(args)
