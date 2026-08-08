"""Footer that only rebuilds when the keys it shows change.

Textual rebuilds every key whenever bindings are refreshed, including the
refreshes fired when the terminal window gains or loses focus. Those
rebuilds retain memory and change nothing on screen.
"""

from __future__ import annotations

from textual.screen import Screen
from textual.widgets import Footer as TextualFooter


def shown_keys(screen: Screen) -> tuple[tuple[str, str, bool], ...]:
    """Key, label and enabled state of every binding the footer shows.

    An unchanged result means a rebuild would draw the same row.
    """
    return tuple(
        (binding.key, binding.description, enabled)
        for _, binding, enabled, _ in screen.active_bindings.values()
        if binding.show
    )


class Footer(TextualFooter):
    """Key hint bar that skips rebuilds that would change nothing."""

    # What the keys on screen were built from. None before the first
    # build, so that one always reaches the parent and draws the row.
    drawn_keys = None

    def bindings_changed(self, screen: Screen) -> None:
        """Rebuild only when the keys have changed."""
        keys = shown_keys(screen)
        if keys != self.drawn_keys:
            self.drawn_keys = keys
            super().bindings_changed(screen)
