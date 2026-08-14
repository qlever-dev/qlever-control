from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.message import Message
from textual.widget import Widget
from textual.widgets import Select, Static


def theme_control_width(themes: list[str]) -> int:
    """Width that fits the longest theme name plus its icon and arrow."""
    longest = max(len(theme) for theme in themes)
    # left padding, leading icon and space, plus the dropdown arrow
    return longest + 7


class ThemeSelect(Select):
    """Dropdown of the app's themes that applies the chosen one on change.

    Compact so it fits the one-row header. The closed control always shows
    the active theme, so the selected theme is visible at a glance.

    The widget stays unaware of the app theme and the table: it posts
    Closed when its dropdown collapses, and an ancestor decides what that
    means (the app applies the picked theme via the built-in Changed).
    """

    class Closed(Message):
        """Posted when the dropdown collapses, after a pick or an escape."""

    def __init__(self, themes: list[str], current: str) -> None:
        """Build the dropdown from `themes`, pre-selecting `current`.

        Each option is prefixed with a theme icon so the closed control
        reads as the theme picker.
        """
        icon = "\N{CIRCLE WITH LEFT HALF BLACK}"
        options = [(f" {icon} {theme}", theme) for theme in themes]
        super().__init__(
            options, value=current, allow_blank=False, compact=True
        )
        self.opened = False

    def on_mount(self) -> None:
        """Watch the open/close so a close can announce itself."""
        self.watch(self, "expanded", self.handle_expanded)

    def handle_expanded(self, expanded: bool) -> None:
        """Post Closed once the dropdown collapses after being opened."""
        if expanded:
            self.opened = True
        elif self.opened:
            self.opened = False
            self.post_message(self.Closed())


class HeaderRow(Horizontal):
    """One-row header: a nav pill left, the title centered, theme picker right.

    The left and center slots are screen-specific and passed in; the right
    slot is always the theme dropdown, the one place it is used. An empty
    slot becomes an empty Static so the layout stays stable.
    """

    def __init__(
        self,
        left: Widget | None = None,
        center: Widget | None = None,
    ) -> None:
        """Build a row with `left` and `center`; the right slot is the theme picker."""
        super().__init__()
        self.left = left if left is not None else Static("")
        self.center = center if center is not None else Static("")
        self.center.add_class("slot-center")

    def compose(self) -> ComposeResult:
        themes = list(self.app.available_themes)
        width = theme_control_width(themes)
        left_slot = Horizontal(self.left, classes="slot-left")
        left_slot.styles.width = width
        yield left_slot
        yield self.center
        select = ThemeSelect(themes, self.app.theme)
        select.styles.width = width
        yield select
