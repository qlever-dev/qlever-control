"""The window's range on one line, with the arrows that move it.

The full-screen plots have no room for the Historic timeline, so this
gives the same shift and jump controls around the range they move.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.widgets import Static

from qlever.monitor_queries.live_data import current_ms
from qlever.monitor_queries.util import format_range
from qlever.monitor_queries.widgets.window_arrows import WindowArrows


class WindowRange(WindowArrows):
    """The range as `« ◄ 10:02:00 → 10:17:00 ► »`, arrows optional.

    A window that cannot move, as on Live, shows the range alone, and
    the arrow methods must not be called.
    """

    def __init__(self, movable: bool) -> None:
        """Remember whether to draw the arrows."""
        super().__init__()
        self.movable = movable

    def compose(self) -> ComposeResult:
        if self.movable:
            yield Static("", id="jump-start-key", classes="key-pill")
            jump_start = Static("«", id="jump-start", classes="range-arrow")
            jump_start.tooltip = "Jump to the start of the log."
            yield jump_start
            yield Static("", id="shift-earlier-key", classes="key-pill")
            shift_earlier = Static(
                "◄", id="shift-earlier", classes="range-arrow"
            )
            shift_earlier.tooltip = "Move the window backward."
            yield shift_earlier
        range_text = Static("", id="range-text")
        range_text.tooltip = "Start and end of the time window the plots show."
        yield range_text
        if self.movable:
            shift_later = Static("►", id="shift-later", classes="range-arrow")
            shift_later.tooltip = "Move the window forward."
            yield shift_later
            yield Static("", id="shift-later-key", classes="key-pill")
            jump_end = Static("»", id="jump-end", classes="range-arrow")
            jump_end.tooltip = "Jump to the end of the log."
            yield jump_end
            yield Static("", id="jump-end-key", classes="key-pill")

    def show_range(self, start_ms: int, end_ms: int) -> None:
        """Write the window's start and end, dated unless both are today."""
        self.query_one("#range-text", Static).update(
            format_range(start_ms, end_ms, current_ms())
        )
