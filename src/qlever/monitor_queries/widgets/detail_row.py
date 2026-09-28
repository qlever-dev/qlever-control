"""The detail pane with the gutter of controls that act on it.

The gutter costs columns rather than rows, because a plot can spare
width far more easily than height.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import NamedTuple

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Button, Static

from qlever.monitor_queries.models import ResourceWindow
from qlever.monitor_queries.widgets.detail_switcher import (
    PLOT_ID,
    DetailSwitcher,
)
from qlever.monitor_queries.widgets.sparql_pane import SparqlPane


class GutterControl(NamedTuple):
    """A gutter glyph, the action it runs, and the name for its ids.

    `hint` is the tooltip, since a glyph and a key say what a control
    is but not what it does. `target` is where the action lives.
    """

    name: str
    glyph: str
    hint: str
    action: str
    target: str = "screen"


# The gutter's controls, one list per pane, each top to bottom.
PLOT_CONTROLS = [
    GutterControl(
        name="to-sparql",
        glyph="≡",
        hint="Show the selected query",
        action="show_sparql",
    ),
    GutterControl(
        name="zoom",
        glyph="⤢",
        hint="Open the plot full screen",
        action="maximize_plot",
    ),
]
SPARQL_CONTROLS = [
    GutterControl(
        name="to-plot",
        glyph="∿",
        hint="Show the resource plot",
        action="show_plot",
    ),
    GutterControl(
        name="copy",
        glyph="⧉",
        hint="Copy the query to the clipboard",
        action="copy_query",
        target="app",
    ),
    GutterControl(
        name="pretty",
        glyph="¶",
        hint="Pretty-print the query",
        action="pretty_print",
        target="app",
    ),
]


def gutter_control(control: GutterControl, pane: str) -> Vertical:
    """Build one control: its glyph button, with its key label below.

    The label's row is always reserved, so turning help mode on never
    moves a glyph.
    """
    glyph = Button(
        control.glyph,
        variant="primary",
        compact=True,
        action=f"{control.target}.{control.action}",
        tooltip=control.hint,
        classes="gutter-glyph",
    )
    # Clicking a control must not take focus off the table.
    glyph.can_focus = False
    return Vertical(
        glyph,
        Static("", id=f"{control.name}-key", classes="gutter-key"),
        classes=f"gutter-control {pane}",
    )


class DetailRow(Horizontal):
    """The detail pane with a gutter of controls down its right."""

    can_focus = False

    def __init__(self, window: ResourceWindow) -> None:
        """Hold the window the plot opens on, to pass to the switcher."""
        super().__init__(id="detail-row")
        self.window = window

    def compose(self) -> ComposeResult:
        yield DetailSwitcher(self.window)
        yield Vertical(
            *(gutter_control(control, "-plot") for control in PLOT_CONTROLS),
            *(
                gutter_control(control, "-sparql")
                for control in SPARQL_CONTROLS
            ),
            classes="pane-gutter",
        )

    def on_mount(self) -> None:
        """Follow the pane, and pick up the one it never announces.

        A ContentSwitcher sets its first pane without going through the
        watcher, so the class has to be taken directly at mount.
        """
        self.watch(self.query_one(DetailSwitcher), "current", self.sync_pane)
        self.sync_pane()

    def sync_pane(self) -> None:
        """Mark which pane is showing, so CSS draws only its controls."""
        showing_plot = self.query_one(DetailSwitcher).current == PLOT_ID
        self.set_class(showing_plot, "-plot")
        self.set_class(not showing_plot, "-sparql")

    def set_help_keys(self, key_for_action: Callable[[str], str]) -> None:
        """Name the key that runs each control, and the pane's own keys."""
        for control in PLOT_CONTROLS + SPARQL_CONTROLS:
            label = self.query_one(f"#{control.name}-key", Static)
            label.update(key_for_action(control.action))
        self.query_one(SparqlPane).set_help_keys(
            key_for_action("scroll_sparql_up")
        )
