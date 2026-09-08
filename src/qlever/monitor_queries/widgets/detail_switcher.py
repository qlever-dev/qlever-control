"""Bottom pane that holds the SPARQL view and the resource plot.

Both panes stay mounted and one is shown, so switching keeps the other's
state: the plot its window, the SPARQL view its scroll position.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.widgets import ContentSwitcher

from qlever.monitor_queries.models import ResourceWindow, SparqlContent
from qlever.monitor_queries.widgets.resource_plot_pane import (
    PLOTS,
    Plot,
    ResourcePlotPane,
)
from qlever.monitor_queries.widgets.sparql_pane import SparqlPane

SPARQL_ID = "sparql-pane"
PLOT_ID = "resource-plot"


class DetailSwitcher(ContentSwitcher):
    """Bottom detail pane showing either the SPARQL or the resource plot.

    Holds both panes and shows one at a time. The screen's r/s bindings
    drive the switch; the footer names the keys, so there is no header
    and nothing here is focusable.
    """

    can_focus = False

    def __init__(self, window: ResourceWindow) -> None:
        super().__init__(initial=PLOT_ID)
        self.window = window

    def compose(self) -> ComposeResult:
        yield SparqlPane(id=SPARQL_ID)
        yield ResourcePlotPane(self.window, PLOTS[0], id=PLOT_ID)

    def show_plot(self, offered: list[Plot]) -> None:
        """Switch to the resource plot, or step to the next one.

        A hidden plot is shown as it was left, so the first press never
        moves it. `offered` is what this log can carry, and a plot no
        longer in it steps back to the first.
        """
        if self.current != PLOT_ID:
            self.current = PLOT_ID
            return
        pane = self.query_one(ResourcePlotPane)
        place = offered.index(pane.plot) if pane.plot in offered else -1
        pane.plot = offered[(place + 1) % len(offered)]

    def show_sparql(self) -> None:
        """Switch to the SPARQL pane."""
        self.current = SPARQL_ID

    def set_sparql(self, content: SparqlContent | None) -> None:
        """Fill the SPARQL pane with the given row's query."""
        self.query_one(SparqlPane).content = content
