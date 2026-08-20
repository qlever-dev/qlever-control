"""Resource plot with a heading row above it.

The heading holds the plot name and the marker legend, which do not fit
inside the plot: plotext neither wraps nor clips, so overflowing text
lands on top of the data.
"""

from __future__ import annotations

from collections.abc import Callable

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import Static

from qlever.monitor_queries.models import ResourcePlot
from qlever.monitor_queries.widgets.resource_plot_pane import (
    ResourcePlotPane,
    point_budget,
    restart_colors,
)

HEADING_ID = "plot-heading"
PLOT_NAME = "Memory and CPU"


def color_markup(color: tuple[int, int, int]) -> str:
    """A Rich color tag for an RGB triplet."""
    return "rgb({}, {}, {})".format(*color)


def heading_text(data: ResourcePlot, dark: bool) -> str:
    """The plot name, then a legend entry per marker kind in the window"""
    stop_color, start_color = restart_colors(dark)
    markers = (
        (data.stop_times_s, "server down", stop_color),
        (data.start_times_s, "server up", start_color),
    )
    parts = [PLOT_NAME, "  "]
    for times, label, color in markers:
        if times:
            parts.append(f"[{color_markup(color)}]│ {label}[/]")
    return " ".join(parts)


class PlotWithHeading(Vertical):
    """Resource plot with a heading row above it.

    Owns the plot's data, its roll timer and its reload trigger, so the
    heading and the plot are drawn from one read of the window.
    Takes a source that returns the points to draw and an optional
    refresh interval. With an interval the plot replots on a timer and
    rolls forward, for the Live window; without one it draws once and
    stays fixed, for a historic span.
    """

    can_focus = False

    def __init__(
        self,
        source: Callable[[], ResourcePlot],
        refresh_interval: float | None = None,
        reload: Callable[[int], None] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source = source
        self.refresh_interval = refresh_interval
        self.reload = reload
        self.last_budget = None

    def compose(self) -> ComposeResult:
        yield Static(id=HEADING_ID)
        yield ResourcePlotPane()

    def on_mount(self) -> None:
        """Draw once; with an interval, also replot on a timer to roll."""
        self.replot()
        if self.refresh_interval is not None:
            self.set_interval(self.refresh_interval, self.replot)
        self.app.theme_changed_signal.subscribe(
            self, lambda theme: self.replot()
        )

    def on_resize(self) -> None:
        """Redraw at the new size, and re-read if the pane got wider.

        A visible pane whose point budget changed asks the owner to
        re-read, so a wider pane shows more detail. A hidden pane has
        width 0 and is skipped.
        """
        self.replot()
        if self.reload is not None and self.size.width > 0:
            budget = point_budget(self.size.width)
            if budget != self.last_budget:
                self.last_budget = budget
                self.reload(budget)

    def replot(self) -> None:
        """Read the window once, then update the heading and the plot.

        Skipped while hidden; being shown fires a resize that replots.
        """
        if not self.display:
            return
        data = self.source()
        heading = self.query_one(f"#{HEADING_ID}", Static)
        heading.update(heading_text(data, self.app.current_theme.dark))
        self.query_one(ResourcePlotPane).draw(data)
