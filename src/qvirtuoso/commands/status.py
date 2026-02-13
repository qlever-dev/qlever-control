from __future__ import annotations

from qoxigraph.commands.status import StatusCommand as QoxigraphStatusCommand


class StatusCommand(QoxigraphStatusCommand):
    DEFAULT_REGEX = "virtuoso-t"

    def description(self) -> str:
        return "Show Virtuoso processes running on this machine"
