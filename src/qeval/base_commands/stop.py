from __future__ import annotations

from abc import abstractmethod

from qeval.base_commands.status import BaseStatusCommand
from qlever.commands.stop import StopCommand as QleverStopCommand


class BaseStopCommand(QleverStopCommand):
    """
    Stop the server of an engine for a given dataset, like QLever's `stop`
    but without systemd: stop its container if there is one, otherwise kill
    the processes that match a regex.
    """

    @abstractmethod
    def default_regex(self) -> str:
        """
        Regex for the command line of the engine's server, where `%%NAME%%`
        stands for the dataset name.
        """

    @abstractmethod
    def status_command(self) -> BaseStatusCommand:
        """The engine's `status` command, shown when no server matches."""

    @abstractmethod
    def status_regex(self) -> str:
        """Regex for the command lines of all the engine's servers."""

    def description(self) -> str:
        return "Stop the server of this graph database for a given dataset"

    def systemd_unit(self, name: str) -> str | None:
        # Only QLever runs its server as a systemd unit so far.
        return None
