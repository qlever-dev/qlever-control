from __future__ import annotations

from textual import events
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.validation import Integer
from textual.widgets import Footer, Input, Label, SelectionList
from textual.widgets.selection_list import Selection

from qlever.monitor_queries.models import FilterState

# Statuses offered in the filter, in display order: the terminal
# statuses a completed query can carry, plus orphaned crash survivors.
FILTER_STATUSES = (
    "ok",
    "failed",
    "timeout",
    "cancelled",
    "orphaned",
)


class FilterModal(ModalScreen[FilterState | None]):
    """Centered modal to filter the Historic table.

    Filters by status, minimum duration, client IP, and SPARQL text.

    Opens pre-filled from the active filters and edits a draft. Enter
    applies the draft, Esc cancels unchanged, c clears the draft.
    """

    BINDINGS = [
        Binding("enter", "apply", "Apply", priority=True),
        Binding("escape", "cancel", "Cancel"),
        Binding("c", "clear", "Clear"),
    ]

    def __init__(self, filters: FilterState) -> None:
        """Hold the active filters to pre-check the selection list."""
        super().__init__()
        self.filters = filters

    def compose(self) -> ComposeResult:
        selections = [
            Selection(
                prompt=status,
                value=status,
                initial_state=status in self.filters.statuses,
            )
            for status in FILTER_STATUSES
        ]
        with Vertical(id="filter-modal"):
            yield Label("Filter", id="filter-title")
            yield Label("Status (space to toggle)", classes="filter-section")
            yield SelectionList(*selections, id="filter-statuses")
            min_duration = self.filters.min_duration_s
            yield Label("Slower than (seconds)", classes="filter-section")
            yield Input(
                value="" if min_duration is None else str(min_duration),
                placeholder="any",
                type="integer",
                validators=[Integer(minimum=0)],
                id="filter-duration",
            )
            yield Label("Client IP contains", classes="filter-section")
            yield Input(
                value=self.filters.client_ip_substr or "",
                placeholder="any",
                id="filter-client-ip",
            )
            yield Label("SPARQL contains", classes="filter-section")
            yield Input(
                value=self.filters.sparql_substr or "",
                placeholder="any",
                id="filter-sparql",
            )
        yield Footer(show_command_palette=False)

    def action_apply(self) -> None:
        """Return a FilterState from the checked statuses and the inputs."""
        selected = self.query_one(SelectionList).selected
        duration = self.query_one("#filter-duration", Input).value.strip()
        min_duration_s = int(duration) if duration.isdigit() else None
        client_ip = self.query_one("#filter-client-ip", Input).value.strip()
        sparql = self.query_one("#filter-sparql", Input).value.strip()
        self.dismiss(
            FilterState(
                statuses=frozenset(selected),
                min_duration_s=min_duration_s,
                client_ip_substr=client_ip or None,
                sparql_substr=sparql or None,
            )
        )

    def action_cancel(self) -> None:
        """Close without changing the active filters."""
        self.dismiss(None)

    def on_click(self, event: events.Click) -> None:
        """Cancel when the dimmed area outside the drawer is clicked."""
        if event.widget is self:
            self.action_cancel()

    def action_clear(self) -> None:
        """Uncheck every status and clear the text drafts."""
        self.query_one(SelectionList).deselect_all()
        self.query_one("#filter-duration", Input).value = ""
        self.query_one("#filter-client-ip", Input).value = ""
        self.query_one("#filter-sparql", Input).value = ""
