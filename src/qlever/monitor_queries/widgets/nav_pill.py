from __future__ import annotations

from textual.message import Message
from textual.widgets import Static


class NavPill(Static):
    """Clickable, non-focusable pill that requests a screen switch."""

    can_focus = False

    class Clicked(Message):
        """Posted on click so an ancestor decides what the click does.

        Bubbles up the DOM, letting the widget stay unaware of the app
        or screen registry.
        """

        def __init__(self, target: str) -> None:
            super().__init__()
            self.target = target

    def __init__(self, label: str, target: str) -> None:
        """Render `label` as the pill; `target` names the screen to switch to."""
        super().__init__(label)
        self.target = target

    def on_click(self) -> None:
        """Announce the click; the handler lives on the parent screen."""
        self.post_message(self.Clicked(self.target))
