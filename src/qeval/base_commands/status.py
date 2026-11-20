from __future__ import annotations

from abc import abstractmethod

from qlever.commands.status import StatusCommand as QleverStatusCommand


class BaseStatusCommand(QleverStatusCommand):
    """
    Show the processes of an engine.
    """

    @abstractmethod
    def default_regex(self) -> str:
        """Regex for the command lines of the engine's processes."""

    def description(self) -> str:
        return (
            "Show the processes of this graph database running on this machine"
        )
