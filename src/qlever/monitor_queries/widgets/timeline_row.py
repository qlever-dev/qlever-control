"""The Historic timeline with the arrows that shift the window and jump
to the log edges, plus the key labels help mode shows beside them.
"""

from __future__ import annotations

from textual import events
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import Static

from qlever.monitor_queries.models import TimelineBounds
from qlever.monitor_queries.widgets.timeline import Timeline


class TimelineRow(Horizontal):
    """The timeline flanked by the controls that move the window.

    The upper control shifts the window by its own width, the lower one
    jumps to the log edge, and both on a side dim together. What is
    possible is read from the times, not the block's position: several
    windows can share one cell, so the block stops moving before the
    window does.
    """

    can_focus = False

    class Shifted(Message):
        """Posted when an arrow is clicked; -1 earlier, +1 later."""

        def __init__(self, direction: int) -> None:
            super().__init__()
            self.direction = direction

    class Jumped(Message):
        """Posted when a jump arrow is clicked; -1 to start, +1 to end."""

        def __init__(self, direction: int) -> None:
            super().__init__()
            self.direction = direction

    bounds = reactive(None, init=False)

    def __init__(self, bounds: TimelineBounds) -> None:
        """Hold the snapshot to hand on to the timeline and controls."""
        super().__init__()
        self.set_reactive(TimelineRow.bounds, bounds)

    def compose(self) -> ComposeResult:
        with Vertical(classes="timeline-controls -left"):
            with Horizontal(classes="timeline-control"):
                yield Static("", id="shift-earlier-key", classes="key-pill")
                yield Static("◄", id="shift-earlier", classes="timeline-arrow")
            with Horizontal(classes="timeline-control"):
                yield Static("", id="jump-start-key", classes="key-pill")
                yield Static("«", id="jump-start", classes="timeline-arrow")
        yield Timeline(self.bounds)
        with Vertical(classes="timeline-controls -right"):
            with Horizontal(classes="timeline-control"):
                yield Static("►", id="shift-later", classes="timeline-arrow")
                yield Static("", id="shift-later-key", classes="key-pill")
            with Horizontal(classes="timeline-control"):
                yield Static("»", id="jump-end", classes="timeline-arrow")
                yield Static("", id="jump-end-key", classes="key-pill")

    def on_mount(self) -> None:
        """Dim the controls for the window we start on."""
        self.sync_controls()

    def watch_bounds(self, bounds: TimelineBounds) -> None:
        """Redraw the timeline and re-dim the controls."""
        self.query_one(Timeline).bounds = bounds
        self.sync_controls()

    def sync_controls(self) -> None:
        """Dim both controls on a side the window cannot move toward."""
        at_start = self.bounds.window_start_ms <= self.bounds.log_start_ms
        at_end = self.bounds.window_end_ms >= self.bounds.log_end_ms
        for control in ("#shift-earlier", "#jump-start"):
            self.query_one(control, Static).disabled = at_start
        for control in ("#shift-later", "#jump-end"):
            self.query_one(control, Static).disabled = at_end

    def set_help_keys(
        self, shift: tuple[str, str], jump: tuple[str, str]
    ) -> None:
        """Name the keys on the shift arrows and on the jump arrows."""
        self.query_one("#shift-earlier-key", Static).update(shift[0])
        self.query_one("#shift-later-key", Static).update(shift[1])
        self.query_one("#jump-start-key", Static).update(jump[0])
        self.query_one("#jump-end-key", Static).update(jump[1])

    def on_click(self, event: events.Click) -> None:
        """Translate a control click into a Shifted or Jumped message."""
        if event.widget is None or event.widget.disabled:
            return
        clicked = event.widget.id
        if clicked == "shift-earlier":
            self.post_message(self.Shifted(-1))
        elif clicked == "shift-later":
            self.post_message(self.Shifted(1))
        elif clicked == "jump-start":
            self.post_message(self.Jumped(-1))
        elif clicked == "jump-end":
            self.post_message(self.Jumped(1))
