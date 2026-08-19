from __future__ import annotations

import re
from pathlib import Path

from qeval.oxigraph.commands.index_stats import IndexStatsCommand
from qlever.containerize import Containerize
from qlever.resource_usage.usage_plot import (
    SUBTITLE_SEPARATOR,
    BandType,
    bands_from_durations,
)
from qlever.util import run_command


def overlay(args, log_path: Path) -> list[BandType]:
    """
    Shade the load and optimize phases of an Oxigraph index build. The
    log has no timestamps, so the phases are assumed to run back to back
    from the build start.
    """
    return bands_from_durations(
        IndexStatsCommand().parse_index_durations(log_path)
    )


def subtitle(args, log_path: Path) -> str | None:
    """Assemble a 'version | read-only' line from the index args."""
    if args.system in Containerize.supported_systems():
        version_cmd = f"{args.system} run --rm {args.image} --version"
    else:
        version_cmd = f"{args.index_binary} --version"
    try:
        version_output = run_command(version_cmd, return_output=True)
    except Exception:
        version_output = ""
    version_match = re.search(r"\d+(?:\.\d+)+", version_output)
    parts = []
    if version_match:
        parts.append(f"{args.index_binary} v{version_match.group()}")
    parts.append(f"read-only = {args.read_only}")
    return SUBTITLE_SEPARATOR.join(parts)
