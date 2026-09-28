from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal

from qlever.monitor_queries.models import ControlsState
from qlever.monitor_queries.widgets.mode_picker import ModePicker
from qlever.monitor_queries.widgets.selected_window import SelectedWindow
from qlever.monitor_queries.widgets.window_stepper import WindowStepper


class HistoricControlsRow(Horizontal):
    """One-row control strip under the HeaderRow on the Historic screen.

    Holds the window stepper, mode picker, and read-only selected range.
    Distributes the snapshot to its children; never reads data itself.
    """

    can_focus = False

    def __init__(self, state: ControlsState) -> None:
        """Hold the controls snapshot to distribute once mounted."""
        super().__init__()
        self.state = state

    def compose(self) -> ComposeResult:
        yield WindowStepper(self.state.window_size)
        yield ModePicker(self.state.mode)
        yield SelectedWindow(self.state)
