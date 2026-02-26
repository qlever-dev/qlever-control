from __future__ import annotations

from qoxigraph.commands.status import StatusCommand as QoxigraphStatusCommand


class StatusCommand(QoxigraphStatusCommand):
    DEFAULT_REGEX = "com.ontotext.graphdb.server.GraphDBServer"

    def description(self) -> str:
        return "Show GraphDB process running on this machine"
