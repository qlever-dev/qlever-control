from __future__ import annotations

from textual import events
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import Static

MODES = ("STARTS", "ACTIVE", "ENDS")


class ModePicker(Horizontal):
    """Segmented mode control: MODE [ STARTS │ ACTIVE │ ENDS ].

    Each mode is its own Static so a segment can be made independently
    clickable without restructuring the widget. The selected segment
    carries the `-selected` state class for styling.
    """

    can_focus = False

    selected = reactive("", init=False)

    class Selected(Message):
        """Posted when a segment is clicked; the screen sets that mode."""

        def __init__(self, mode: str) -> None:
            super().__init__()
            self.mode = mode

    def __init__(self, selected: str) -> None:
        """Hold the selected mode to paint once mounted."""
        super().__init__()
        self.set_reactive(ModePicker.selected, selected)

    def compose(self) -> ComposeResult:
        caption = Static("MODE", classes="picker-caption")
        caption.tooltip = (
            "Which queries in the window to show - STARTS began in it, "
            "ACTIVE ran during it, ENDS finished in it."
        )
        yield caption
        yield Static("[", classes="picker-bracket")
        for index, mode in enumerate(MODES):
            if index > 0:
                yield Static("│", classes="picker-sep")
            classes = "picker-segment"
            if mode == self.selected:
                classes += " -selected"
            yield Static(mode, id=f"mode-{mode.lower()}", classes=classes)
        yield Static("]", classes="picker-bracket")

    def watch_selected(self, value: str) -> None:
        """Move the -selected state class onto the chosen segment."""
        for mode in MODES:
            segment = self.query_one(f"#mode-{mode.lower()}", Static)
            segment.set_class(mode == value, "-selected")

    def on_click(self, event: events.Click) -> None:
        """Translate a segment click into a Selected message."""
        clicked = event.widget.id if event.widget else None
        if clicked and clicked.startswith("mode-"):
            mode = clicked.removeprefix("mode-").upper()
            if mode in MODES:
                self.post_message(self.Selected(mode))
