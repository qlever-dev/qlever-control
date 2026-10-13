from __future__ import annotations

from qeval.base_commands.stop import BaseStopCommand
from qeval.oxigraph.commands.status import StatusCommand


class StopCommand(BaseStopCommand):
    """Stop the Oxigraph server for a given dataset."""

    def default_regex(self) -> str:
        return r"^(\S*/)?oxigraph\s+serve.*%%NAME%%_index"

    def status_command(self) -> StatusCommand:
        return StatusCommand()
