from __future__ import annotations

from qoxigraph.commands.status import StatusCommand as QoxigraphStatusCommand


class StatusCommand(QoxigraphStatusCommand):
    DEFAULT_REGEX = "fuseki-server"

    def description(self) -> str:
        return "Show Jena fuseki-server processes running on this machine"
