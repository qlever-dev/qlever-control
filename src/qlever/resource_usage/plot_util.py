"""
Resource-usage plot code shared by all engines. Kept apart from
`usage_plot`, so that it can be imported without the optional plotting
libraries.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from qlever.log import log

# One shaded region of the plot: name, start and end in seconds.
BandType = tuple[str, float, float]

# The engine-specific parts of the plot, called with `args` and the path
# of the index log.
OverlayType = Callable[[Any, Path], list[BandType]]
SubtitleType = Callable[[Any, Path], str | None]

# Separator between the fields of a subtitle.
SUBTITLE_SEPARATOR = "   |   "


def bands_from_durations(
    durations: dict[str, float],
) -> list[BandType]:
    """
    Turn phase durations in seconds into `(label, start_s, end_s)` bands,
    laying the phases back to back from the build start in the given
    order. The `TOTAL time` entry is skipped.
    """
    bands = []
    start_s = 0.0
    for label, duration_s in durations.items():
        if label == "TOTAL time":
            continue
        bands.append((label, start_s, start_s + duration_s))
        start_s += duration_s
    return bands


def render_usage_plot_if_installed(
    args,
    *,
    plot_only: bool,
    engine_overlay: OverlayType | None = None,
    engine_subtitle: SubtitleType | None = None,
) -> Path | None:
    """
    Render the resource-usage plot with `usage_plot.render_usage_plot`.
    The plotting libraries are an optional dependency, so the import has
    to happen here and not at module level. Missing them is an error when
    the user asked for the plot directly via `plot_only`, and only a hint
    otherwise, because then the index build itself succeeded.
    """
    try:
        from qlever.resource_usage import usage_plot
    except ImportError:
        if plot_only:
            log.error(
                "Resource-usage plot needs matplotlib and numpy "
                "(`pip install qlever[plot]`). Install them and rerun."
            )
        else:
            log.info(
                "To plot the resource-usage log, install matplotlib and "
                "numpy (`pip install qlever[plot]`), then run "
                f"`{args.main_command_name} index --resource-usage-plot-only`."
            )
        return None
    return usage_plot.render_usage_plot(
        args,
        engine_overlay=engine_overlay,
        engine_subtitle=engine_subtitle,
    )
