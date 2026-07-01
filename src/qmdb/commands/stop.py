from __future__ import annotations

from qmdb.commands.status import StatusCommand
from qoxigraph.commands.stop import StopCommand as QoxigraphStopCommand


class StopCommand(QoxigraphStopCommand):
    """
    Stop a running MillenniumDB server. Matches the mdb server process
    by its dataset name argument so that only the server for the given
    dataset is stopped.
    """

    STATUS_COMMAND = StatusCommand()
    # %%NAME%% is replaced with args.name at execution time
    DEFAULT_REGEX = r"mdb\s+server.*%%NAME%%_index"

    def description(self) -> str:
        return "Stop MillenniumDB server for a given dataset"
