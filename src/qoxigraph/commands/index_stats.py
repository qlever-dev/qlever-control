from __future__ import annotations

import re

import qlever.util as util
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


class IndexStatsCommand(QleverIndexStatsCommand):
    """
    Show index build time and disk space usage for an Oxigraph dataset.
    Time is read from the "Total elapsed time" line appended to the
    index log by the index command; space is the sum of all .sst files.
    """

    def execute_time(
        self, args, log_file_name: str
    ) -> dict[str, tuple[float | None, str]]:
        """Parse total index build time from the index log file."""
        try:
            # Read the last few lines of the log file (the total time is
            # always near the end).
            log_text = util.run_command(
                f"tail {log_file_name}", return_output=True
            )
        except Exception as e:
            log.error(f"Problem reading index log file {log_file_name}: {e}")
            return {}

        stats = {}
        # Pattern: "Total elapsed time: <number>s" (total time, always last)
        total_pattern = re.compile(r"Total elapsed time: ([\d,]+)s$")

        for line in log_text.splitlines():
            match = total_pattern.search(line)
            if not match:
                continue

            try:
                value_s = float(match.group(1).replace(",", ""))
            except (ValueError, TypeError):
                continue

            time_unit = get_time_unit(args.time_unit, value_s)
            unit_factor = get_time_unit_factor(time_unit)

            stats["TOTAL time"] = (value_s / unit_factor, time_unit)
            break

        return stats

    def execute_space(self, args) -> dict[str, tuple[float, str]]:
        """
        Return the space used by the index files (*.sst) along with the unit.
        """
        index_size = util.get_total_file_size([f"{args.name}_index/*.sst"])

        size_unit = get_size_unit(args.size_unit, index_size)
        unit_factor = get_size_unit_factor(size_unit)

        index_size /= unit_factor

        return {"TOTAL size": (index_size, size_unit)}
