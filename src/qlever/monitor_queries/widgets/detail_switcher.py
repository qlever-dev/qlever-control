"""Bottom pane that holds the SPARQL view and the resource plot.

Both panes stay mounted and one is shown, so switching keeps the other's
state: the plot its rolling timer, the SPARQL view its scroll position.
"""

from __future__ import annotations

from collections.abc import Callable

from textual.app import ComposeResult
from textual.widgets import ContentSwitcher

from qlever.monitor_queries.models import ResourcePlot, SparqlContent
from qlever.monitor_queries.widgets.resource_plot_pane import ResourcePlotPane
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

    def __init__(
        self,
        source: Callable[[], ResourcePlot],
        refresh_interval: float | None = None,
        reload: Callable[[int], None] | None = None,
    ) -> None:
        super().__init__(initial=PLOT_ID)
        self.source = source
        self.refresh_interval = refresh_interval
        self.reload = reload

    def compose(self) -> ComposeResult:
        yield SparqlPane(id=SPARQL_ID)
        yield ResourcePlotPane(
            self.source, self.refresh_interval, self.reload, id=PLOT_ID
        )

    def show_plot(self) -> None:
        """Switch to the resource plot pane."""
        self.current = PLOT_ID

    def show_sparql(self) -> None:
        """Switch to the SPARQL pane."""
        self.current = SPARQL_ID

    def set_sparql(self, content: SparqlContent | None) -> None:
        """Fill the SPARQL pane with the given row's query."""
        self.query_one(SparqlPane).content = content

    def replot(self) -> None:
        """Redraw the plot; a no-op while the SPARQL pane is shown."""
        self.query_one(ResourcePlotPane).replot()
