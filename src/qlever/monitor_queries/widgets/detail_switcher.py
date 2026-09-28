"""Bottom pane that holds the SPARQL view and the resource plot.

Both panes stay mounted and one is shown, so switching keeps the other's
state: the plot its window, the SPARQL view its scroll position.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.widgets import ContentSwitcher

from qlever.monitor_queries.models import ResourceWindow, SparqlContent
from qlever.monitor_queries.widgets.resource_plot_pane import ResourcePlotPane
from qlever.monitor_queries.widgets.sparql_pane import SparqlPane

SPARQL_ID = "sparql-pane"
PLOT_ID = "resource-plot"


class DetailSwitcher(ContentSwitcher):
    """Bottom detail pane showing either the SPARQL or the resource plot.

    Holds both panes and shows one at a time. The gutter beside it and
    the r/s keys drive the switch, so there is no header here and
    nothing is focusable.
    """

    can_focus = False

    def __init__(self, window: ResourceWindow) -> None:
        super().__init__(initial=PLOT_ID)
        self.window = window

    def compose(self) -> ComposeResult:
        yield SparqlPane(id=SPARQL_ID)
        yield ResourcePlotPane(self.window, id=PLOT_ID)

    def show_plot(self) -> None:
        """Switch to the resource plot pane."""
        self.current = PLOT_ID

    def show_sparql(self) -> None:
        """Switch to the SPARQL pane."""
        self.current = SPARQL_ID

    def set_sparql(self, content: SparqlContent | None) -> None:
        """Fill the SPARQL pane with the given row's query."""
        self.query_one(SparqlPane).content = content
