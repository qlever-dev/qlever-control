"""Full-screen modal showing the resource plot.

The pane inside does the drawing. This modal only frames it and closes
it. The bigger pane fits more buckets, and it says so when it resizes,
so the screen that holds the readings can send a more detailed window.
"""

from __future__ import annotations

from textual import events
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen

from qlever.monitor_queries.models import ResourceWindow
from qlever.monitor_queries.widgets.footer import Footer
from qlever.monitor_queries.widgets.resource_plot_pane import ResourcePlotPane


class ResourcePlotModal(ModalScreen):
    """Shows the resource plot full screen.

    Opens on the window the inline pane is showing. After that the
    screen keeps it up to date: Historic sends the window it re-reads at
    this size, and Live sends fresh readings on its timer.
    """

    BINDINGS = [
        # One footer entry for the pair: the key that opened it closes it.
        Binding("escape", "close", "Close", key_display="esc/z"),
        Binding("z", "close", "Close", show=False),
        # A modal cuts the app's bindings, so quit is repeated here.
        Binding("q", "app.quit", "Quit"),
    ]

    def __init__(self, window: ResourceWindow) -> None:
        super().__init__()
        self.window = window

    def compose(self) -> ComposeResult:
        with Vertical(id="resource-plot-modal"):
            yield ResourcePlotPane(self.window)
        yield Footer(show_command_palette=False)

    def action_close(self) -> None:
        """Close the modal, unless a prior event already closed it."""
        if self.is_current:
            self.dismiss()

    def on_click(self, event: events.Click) -> None:
        """Close when the dimmed area outside the plot is clicked."""
        if event.widget is self:
            self.action_close()
