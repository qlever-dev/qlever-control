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
        # Read the last few lines of the log file (the total time is
        # always near the end).
        try:
            log_text = run_command(
                f"tail {log_file_name}", return_output=True
            )
        except Exception as e:
            log.error(f"Problem reading index log file {log_file_name}: {e}")
            return {}

        stats = {}
        # Pattern: "Finished in <number>ms" (total time, always last)
        total_pattern = re.compile(r"Finished in ([\d,]+)ms\s*$")

        for line in log_text.splitlines():
            match = total_pattern.search(line)
            if not match:
                continue

            try:
                value_s = float(match.group(1).replace(",", "")) / 1000
            except (ValueError, TypeError):
                continue

            time_unit = self.get_time_unit(args.time_unit, value_s)
            unit_factor = self.get_time_unit_factor(time_unit)

            stats["TOTAL time"] = (value_s / unit_factor, time_unit)
            break

        return stats

    def execute_space(self, args) -> dict[str, tuple[float, str]]:
        """
        Part of `execute` that returns the space used by different types of
        index along with the unit.
        """
        storage = f"{args.name}_index/data/repositories/{args.name}/storage"
        index_size = get_total_file_size(
            [f"{storage}/*", f"{storage}/literals-index/*"],
            exclude={"owlim.properties", "rule.list", "last_precommit_id"},
        )

        size_unit = self.get_size_unit(args.size_unit, index_size)
        unit_factor = self.get_size_unit_factor(size_unit)

        index_size /= unit_factor

        return {"TOTAL size": (index_size, size_unit)}
