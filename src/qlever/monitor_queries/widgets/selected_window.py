from __future__ import annotations

from textual.reactive import reactive
from textual.widgets import Static

from qlever.monitor_queries.models import ControlsState
from qlever.monitor_queries.util import format_clock


def format_selected_window(state: ControlsState) -> str:
    """Read-only summary of the current window's range and width."""
    start = format_clock(state.start_ms)
    end = format_clock(state.end_ms)
    return f"[b]SELECTED[/b]  {start} → {end}  ([b]{state.window_size}[/b])"


class SelectedWindow(Static):
    """Read-only window range under the HeaderRow on the Historic screen."""

    can_focus = False

    state = reactive(None, init=False)

    def __init__(self, state: ControlsState) -> None:
        """Render the selected window range computed from `state`."""
        super().__init__(format_selected_window(state))
        self.set_reactive(SelectedWindow.state, state)

    def watch_state(self, value: ControlsState) -> None:
        """Repaint the range summary when the window state changes."""
        self.update(format_selected_window(value))
