"""The arrows that shift a window and jump to the log edges.

The Historic timeline row and the range line of the full-screen plot view
both carry them, so what the arrows do lives here and each row only
lays them out.
"""

from __future__ import annotations

from textual import events
from textual.containers import Horizontal
from textual.message import Message
from textual.widgets import Static


class WindowArrows(Horizontal):
    """A row with the four window arrows, laid out by a subclass.

    The subclass composes the arrows as Statics with the ids
    `shift-earlier`, `shift-later`, `jump-start` and `jump-end`, each
    with a `<id>-key` pill for help mode, around whatever it shows
    between them.
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

    def set_edges(self, at_start: bool, at_end: bool) -> None:
        """Grey both arrows on a side the window cannot move toward."""
        for arrow in ("#shift-earlier", "#jump-start"):
            self.query_one(arrow, Static).disabled = at_start
        for arrow in ("#shift-later", "#jump-end"):
            self.query_one(arrow, Static).disabled = at_end

    def set_help_keys(
        self, shift: tuple[str, str], jump: tuple[str, str]
    ) -> None:
        """Name the keys on the shift arrows and on the jump arrows."""
        self.query_one("#shift-earlier-key", Static).update(shift[0])
        self.query_one("#shift-later-key", Static).update(shift[1])
        self.query_one("#jump-start-key", Static).update(jump[0])
        self.query_one("#jump-end-key", Static).update(jump[1])

    def on_click(self, event: events.Click) -> None:
        """Translate an arrow click into a Shifted or Jumped message."""
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
