"""Full-screen modal showing the resource plot.

The pane inside owns the drawing; the modal only handles closing. A
historic plot re-reads its window at the modal's wider width, so
maximizing adds detail instead of stretching the inline pane's points.
"""

from __future__ import annotations

from collections.abc import Callable

from textual import events, work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.worker import get_current_worker

from qlever.monitor_queries.models import ResourcePlot
from qlever.monitor_queries.widgets.footer import Footer
from qlever.monitor_queries.widgets.resource_plot_pane import ResourcePlotPane


class ResourcePlotModal(ModalScreen):
    """Shows the resource plot full screen.

    The pane draws itself; this screen only frames it and closes it.
    Historic passes a reader, so the window is read again at the bigger
    size and the plot gains detail. Live reads from its buffer instead.
    """

    BINDINGS = [Binding("escape", "close", "Close")]

    def __init__(
        self,
        source: Callable[[], ResourcePlot],
        refresh_interval: float | None = None,
        reader: Callable[[int, Callable[[], bool]], ResourcePlot]
        | None = None,
    ) -> None:
        super().__init__()
        self.source = source
        self.refresh_interval = refresh_interval
        self.reader = reader
        self.plot = None

    def compose(self) -> ComposeResult:
        reload = self.load_plot if self.reader is not None else None
        with Vertical(id="resource-plot-modal"):
            yield ResourcePlotPane(
                self.pane_source, self.refresh_interval, reload
            )
        yield Footer(show_command_palette=False)

    def pane_source(self) -> ResourcePlot:
        """Draw the re-read plot once we have it, else the initial one."""
        if self.plot is not None:
            return self.plot
        return self.source()

    @work(thread=True, exclusive=True)
    def load_plot(self, max_points: int) -> None:
        """Re-read the window at this width off the UI thread.

        The pane calls this on resize. Exclusive, so a resize while a read
        is in flight cancels it and only the final width lands. Drops the
        result if the modal closed while reading.
        """
        worker = get_current_worker()
        plot = self.reader(max_points, lambda: worker.is_cancelled)
        if worker.is_cancelled or not self.is_current:
            return
        self.app.call_from_thread(self.apply_plot, plot)

    def apply_plot(self, plot: ResourcePlot) -> None:
        """Store the re-read plot and redraw the pane."""
        self.plot = plot
        self.query_one(ResourcePlotPane).replot()

    def action_close(self) -> None:
        """Close the modal, unless a prior event already closed it."""
        if self.is_current:
            self.dismiss()

    def on_click(self, event: events.Click) -> None:
        """Close when the dimmed area outside the plot is clicked."""
        if event.widget is self:
            self.action_close()
