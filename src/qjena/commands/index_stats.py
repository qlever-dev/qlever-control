from __future__ import annotations

import re

from qlever.commands.index_stats import (
    IndexStatsCommand as QleverIndexStatsCommand,
)
from qlever.log import log
from qlever.util import get_total_file_size, run_command


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
            log_text = run_command(
                f"tail -n 20 {log_file_name}", return_output=True
            )
        except Exception as e:
            log.error(f"Problem reading index log file {log_file_name}: {e}")
            return {}

        stats = {}
        # Pattern: "<label> = <number> seconds"
        pattern = re.compile(r"INFO\s+(.*?)\s*=\s*([\d.]+)\s+seconds")
        # Pattern for the overall line in seconds
        overall_pattern = re.compile(r"INFO\s+Overall\s+(\d+)\s+seconds")

        for line in log_text.splitlines():
            label = raw_value = None
            overall_match = overall_pattern.search(line)
            if overall_match:
                label = "TOTAL time"
                raw_value = overall_match.group(1)
            else:
                match = pattern.search(line)
                if match:
                    label = match.group(1).strip()
                    raw_value = match.group(2)

            if raw_value is None:
                continue

            try:
                value = float(raw_value)
            except (ValueError, TypeError):
                continue

            time_unit = self.get_time_unit(args.time_unit, value)
            unit_factor = self.get_time_unit_factor(time_unit)

            normalized_value = value / unit_factor
            stats[label] = (normalized_value, time_unit)

            if overall_match:
                break

        return stats

    def execute_space(self, args) -> dict[str, tuple[float, str]]:
        """
        Part of `execute` that returns the space used by different types of
        index along with the unit.
        """
        index_size = get_total_file_size(["index/Data-0001/*"])

        size_unit = self.get_size_unit(args.size_unit, index_size)
        unit_factor = self.get_size_unit_factor(size_unit)

        index_size /= unit_factor

        return {"TOTAL size": (index_size, size_unit)}
