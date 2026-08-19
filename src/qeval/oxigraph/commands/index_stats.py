from __future__ import annotations

import re
from pathlib import Path

from qlever.commands.index_stats import (
    IndexStatsCommand as QleverIndexStatsCommand,
)
from qlever.commands.index_stats import (
    get_size_unit,
    get_size_unit_factor,
    get_time_unit,
    get_time_unit_factor,
)
from qlever.log import log
from qlever.util import get_total_file_size, run_command

PHASE_LABELS = ["Load time", "Optimize time", "TOTAL time"]


class IndexStatsCommand(QleverIndexStatsCommand):
    """
    Show how long the index build took and how much space the index uses,
    for an Oxigraph dataset.

    This is also the base class for the other new engines, which all read
    the times from their index log rather than from timestamps the way
    QLever does. They override the two methods below; the defaults here
    are Oxigraph's own, as with `DEFAULT_REGEX` in `stop.py`.
    """

    def index_size_patterns(self, args) -> list[str]:
        """The index files to add up for the space report."""
        return [f"{args.name}_index/*.sst"]

    def parse_index_durations(
        self, log_file_name: str | Path
    ) -> dict[str, float]:
        """
        How long each phase of the build took, in seconds, in the order
        they should be shown. Empty if the log cannot be read.

        A build with no optimize step reports only its total, because then
        loading is the whole build rather than one phase of it.
        """
        try:
            # The times are always near the end of the log.
            log_text = run_command(f"tail {log_file_name}", return_output=True)
        except Exception as e:
            log.error(f"Problem reading index log file {log_file_name}: {e}")
            return {}

        durations = {}
        for label in PHASE_LABELS:
            match = re.search(rf"{re.escape(label)}:\s*(\d+)s", log_text)
            if match:
                durations[label] = float(match.group(1))
        if "Optimize time" not in durations:
            durations.pop("Load time", None)
        return durations

    def execute_time(
        self, args, log_file_name: str
    ) -> dict[str, tuple[float | None, str]]:
        """
        Show how long each phase took, all in the same unit, picked to
        suit the longest phase.
        """
        durations = self.parse_index_durations(log_file_name)
        if not durations:
            return {}

        time_unit = get_time_unit(args.time_unit, max(durations.values()))
        unit_factor = get_time_unit_factor(time_unit)

        return {
            label: (seconds / unit_factor, time_unit)
            for label, seconds in durations.items()
        }

    def execute_space(self, args) -> dict[str, tuple[float, str]]:
        """Show how much space the index files use, and in which unit."""
        index_size = get_total_file_size(self.index_size_patterns(args))

        size_unit = get_size_unit(args.size_unit, index_size)
        unit_factor = get_size_unit_factor(size_unit)

        index_size /= unit_factor

        return {"TOTAL size": (index_size, size_unit)}
