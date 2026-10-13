from __future__ import annotations

from abc import abstractmethod
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
from qlever.util import get_total_file_size


def read_index_log(log_file_name: str | Path) -> str | None:
    """
    The text of the index log, or None if it cannot be read, which is
    reported as an error.
    """
    try:
        return Path(log_file_name).read_text(errors="replace")
    except OSError as e:
        log.error(f"Problem reading index log file {log_file_name}: {e}")
        return None


class BaseIndexStatsCommand(QleverIndexStatsCommand):
    """
    Show how long the index build of an engine took and how much space its
    index uses, from the durations in its index log and the size of its
    index files.
    """

    @abstractmethod
    def index_size_patterns(self, args) -> list[str]:
        """The glob patterns of the index files, whose sizes add up."""

    @abstractmethod
    def parse_index_durations(self, log_text: str) -> dict[str, float]:
        """
        How long each phase of the build took in seconds, from the text of
        the index log, in the order to show them.
        """

    def index_durations(self, log_file_name: str | Path) -> dict[str, float]:
        """
        Read the index log and parse it. Empty if the log cannot be read.
        """
        log_text = read_index_log(log_file_name)
        if log_text is None:
            return {}
        return self.parse_index_durations(log_text)

    def execute_time(
        self, args, log_file_name: str
    ) -> dict[str, tuple[float | None, str]]:
        """
        The duration of each phase, all in the same unit, picked to suit
        the longest one.
        """
        durations = self.index_durations(log_file_name)
        if not durations:
            return {}
        time_unit = get_time_unit(args.time_unit, max(durations.values()))
        unit_factor = get_time_unit_factor(time_unit)
        return {
            heading: (seconds / unit_factor, time_unit)
            for heading, seconds in durations.items()
        }

    def execute_space(self, args) -> dict[str, tuple[float, str]]:
        """The total size of the index files, in a unit that suits it."""
        index_size = get_total_file_size(self.index_size_patterns(args))
        size_unit = get_size_unit(args.size_unit, index_size)
        unit_factor = get_size_unit_factor(size_unit)
        return {"TOTAL size": (index_size / unit_factor, size_unit)}
