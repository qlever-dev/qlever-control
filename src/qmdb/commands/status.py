from __future__ import annotations

from qoxigraph.commands.status import StatusCommand as QoxigraphStatusCommand


class StatusCommand(QoxigraphStatusCommand):
    """Show running MillenniumDB server processes by matching the process name."""

    DEFAULT_REGEX = "mdb\\s+server"

    def description(self) -> str:
        return "Show MillenniumDB processes running on this machine"
