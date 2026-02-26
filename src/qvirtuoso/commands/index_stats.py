from __future__ import annotations

import re
from datetime import datetime

from qlever.commands.index_stats import (
    IndexStatsCommand as QleverIndexStatsCommand,
)
from qlever.log import log
from qlever.util import get_total_file_size


class IndexStatsCommand(QleverIndexStatsCommand):
    """
    Show index build time and disk space for a Virtuoso index.
    """

    def execute_time(
        self, args, log_file_name: str
    ) -> dict[str, tuple[float | None, str]]:
        """
        Parse the Virtuoso index log to compute build time. Handles multiple
        loading runs (initial + extend) by tracking state transitions:
        IDLE -> LOADING (on "Loader started") -> WAITING_CHECKPOINT
        (on "Loader has finished") -> IDLE (on "Checkpoint finished").
        Each completed cycle is one measured run.
        """
        try:
            with open(log_file_name, "r") as f:
                lines = f.readlines()
        except Exception as e:
            log.error(f"Problem reading index log file {log_file_name}: {e}")
            return {}

        # Virtuoso runs parallel loaders and the log may contain multiple
        # server runs (initial index + extend). For each run we want:
        #   first "Loader started" -> first "Checkpoint finished" after
        #   "Loader has finished".
        #
        # State switching: IDLE -> LOADING -> WAITING_CHECKPOINT -> IDLE
        #
        # The log has date headers ("\t\tMon Feb 16 2026") followed by
        # timestamped lines ("HH:MM:SS ..."). We track the current date
        # so that timestamps spanning midnight are handled correctly.
        timestamp_pattern = re.compile(r"^(\d{2}:\d{2}:\d{2})\s")
        date_pattern = re.compile(r"^\t\t\w+ (\w+ \d+ \d{4})")
        IDLE, LOADING, WAITING_CHECKPOINT = range(3)
        state = IDLE
        current_date = None
        start_time = None
        run_seconds = []

        for line in lines:
            date_match = date_pattern.match(line)
            if date_match:
                current_date = datetime.strptime(
                    date_match.group(1), "%b %d %Y"
                ).date()
                continue

            ts_match = timestamp_pattern.match(line)
            if not ts_match or current_date is None:
                continue
            ts = datetime.combine(
                current_date,
                datetime.strptime(ts_match.group(1), "%H:%M:%S").time(),
            )

            if state == IDLE and "Loader started" in line:
                start_time = ts
                state = LOADING
            elif state == LOADING and "Loader has finished" in line:
                state = WAITING_CHECKPOINT
            elif (
                state == WAITING_CHECKPOINT
                and "Checkpoint finished" in line
                and start_time
            ):
                run_seconds.append((ts - start_time).total_seconds())
                state = IDLE

        if not run_seconds:
            return {}

        total_seconds = sum(run_seconds)
        time_unit = self.get_time_unit(args.time_unit, total_seconds)
        unit_factor = self.get_time_unit_factor(time_unit)

        stats = {}
        if len(run_seconds) > 1:
            for i, seconds in enumerate(run_seconds):
                stats[f"Index build {i + 1}"] = (seconds / unit_factor, time_unit)
        stats["TOTAL time"] = (total_seconds / unit_factor, time_unit)

        return stats

    def execute_space(self, args) -> dict[str, tuple[float, str]]:
        """
        Part of `execute` that returns the space used by different types of
        index along with the unit.
        """
        index_size = get_total_file_size(["virtuoso.db"])

        size_unit = self.get_size_unit(args.size_unit, index_size)
        unit_factor = self.get_size_unit_factor(size_unit)

        index_size /= unit_factor

        return {"TOTAL size": (index_size, size_unit)}
