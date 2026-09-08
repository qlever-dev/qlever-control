"""Full-screen modal showing every resource plot at once.

The panes inside do the drawing. This modal only stacks them, frames
them and closes them. They all read the same window, so the same moment
in time is at the same place in each, and the whole stack is one read:
a bigger pane fits more buckets and says so when it resizes, so the
screen that holds the readings can send a more detailed window.
"""

from __future__ import annotations

from textual import events
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen

from qlever.monitor_queries.models import ResourceWindow
from qlever.monitor_queries.widgets.footer import Footer
from qlever.monitor_queries.widgets.resource_plot_pane import (
    Plot,
    ResourcePlotPane,
    label_width,
)


class ResourcePlotModal(ModalScreen):
    """Shows the plots this log can carry, stacked, full screen.

    Opens on the window the inline pane is showing. After that the
    screen keeps every pane up to date: Historic sends the window it
    re-reads at this size, and Live sends fresh readings on its timer.
    """

    # `R` closes again, so the key that opened the modal toggles it.
    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("R", "close", "Close", show=False),
    ]

    def __init__(self, window: ResourceWindow, plots: list[Plot]) -> None:
        super().__init__()
        self.window = window
        self.plots = plots

    def compose(self) -> ComposeResult:
        # One width for the whole stack, since a pane only knows its own
        # numbers and the gutters have to come out the same size.
        width = label_width(self.window, self.plots)
        with Vertical(id="resource-plot-modal"):
            for plot in self.plots:
                yield ResourcePlotPane(
                    self.window,
                    plot,
                    time_labels=plot is self.plots[-1],
                    label_width=width,
                )
        yield Footer(show_command_palette=False)

    def action_close(self) -> None:
        """Close the modal, unless a prior event already closed it."""
        if self.is_current:
            self.dismiss()

    def on_click(self, event: events.Click) -> None:
        """Close when the dimmed area outside the plot is clicked."""
        if event.widget is self:
            self.action_close()
