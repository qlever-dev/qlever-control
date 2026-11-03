from __future__ import annotations

import re

from qeval.base_commands.index_stats import BaseIndexStatsCommand


class IndexStatsCommand(BaseIndexStatsCommand):
    """
    Show how long the Oxigraph index build took and how much space the
    index uses.
    """

    def index_size_patterns(self, args) -> list[str]:
        return [f"{args.name}_index/*.sst"]

    def parse_index_durations(self, log_text: str) -> dict[str, float]:
        """
        The times that `index` writes at the end of the log. A build with
        no optimize step reports only its total, because then loading is
        the whole build rather than one phase of it.
        """
        durations = {}
        for label in ("Load time", "Optimize time", "TOTAL time"):
            match = re.search(rf"{re.escape(label)}:\s*(\d+)s", log_text)
            if match:
                durations[label] = float(match.group(1))
        if "Optimize time" not in durations:
            durations.pop("Load time", None)
        return durations
