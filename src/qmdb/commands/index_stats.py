from __future__ import annotations

import re

from qlever.commands.index_stats import (
    IndexStatsCommand as QleverIndexStatsCommand,
)
from qlever.log import log
from qlever.util import get_total_file_size


class IndexStatsCommand(QleverIndexStatsCommand):
    """
    Class for executing the `index-stats` command.
    """

    def execute_time(
        self, args, log_file_name: str
    ) -> dict[str, tuple[float | None, str]]:
        """
        Part of `execute` that returns the time used for each part of indexing
        along with the unit.
        """

        # Read the content of `log_file_name` into a list of lines.
        try:
            with open(log_file_name, "r") as log_file:
                lines = log_file.readlines()
        except Exception as e:
            log.error(f"Problem reading index log file {log_file_name}: {e}")
            return {}

        stats = {}
        # Pattern: "<label> = <number> seconds"
        pattern = re.compile(
            r"^(.*?)\s*duration:\s*([\d.]+)\s*(milliseconds|seconds|minutes|hours)",
            re.IGNORECASE
        )

        unit_to_seconds = {
            "milliseconds": 1 / 1000,
            "seconds":      1,
            "minutes":      60,
            "hours":        3600,
        }

        for line in lines:
            label = raw_value = mdb_time_unit = None
            match = pattern.search(line)
            if match:
                label = match.group(1).strip()
                if label.lower() == "total import":
                    label = "TOTAL time"
                raw_value = match.group(2)
                mdb_time_unit = match.group(3).lower()

            if raw_value is None or mdb_time_unit is None:
                continue

            try:
                value = float(raw_value)
            except (ValueError, TypeError):
                continue

            factor = unit_to_seconds.get(mdb_time_unit)
            if factor is None:
                continue

            value_s = value * factor

            time_unit = self.get_time_unit(args.time_unit, value_s)
            unit_factor = self.get_time_unit_factor(time_unit)

            normalized_value = value_s / unit_factor
            stats[label] = (normalized_value, time_unit)

        return stats

    def execute_space(self, args) -> dict[str, tuple[float, str]]:
        """
        Part of `execute` that returns the space used by different types of
        index along with the unit.
        """
        index_size = get_total_file_size([f"{args.name}_index/*"])

        size_unit = self.get_size_unit(args.size_unit, index_size)
        unit_factor = self.get_size_unit_factor(size_unit)

        index_size /= unit_factor

        return {"TOTAL size": (index_size, size_unit)}
