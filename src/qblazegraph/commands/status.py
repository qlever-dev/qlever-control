from __future__ import annotations

from qoxigraph.commands.status import StatusCommand as QoxigraphStatusCommand


class StatusCommand(QoxigraphStatusCommand):
    DEFAULT_REGEX = "java\\s+-server.*blazegraph.jar"

    def description(self) -> str:
        return (
            "Show Java processes with blazegraph.jar running on this machine"
        )
