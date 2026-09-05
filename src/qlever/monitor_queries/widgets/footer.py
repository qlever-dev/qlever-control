"""Footer that only rebuilds when the keys it shows change.

Textual rebuilds every key whenever bindings are refreshed, including the
refreshes fired when the terminal window gains or loses focus. Those
rebuilds change nothing on screen, and the replaced key widgets are never
freed (verified with Textual 8.2.8: 200 redundant refreshes leave 400 dead
`FooterKey` widgets behind, surviving a forced garbage collection), which
over a long session degrades the app into a flickering, frozen state.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Footer as TextualFooter

# Key, displayed key, label, enabled state and tooltip of one shown
# binding: everything a footer row is drawn from (a click simulates the
# key, so the action does not matter here).
ShownKey = tuple[str, str, str, bool, str]


def shown_keys(screen: Screen) -> tuple[ShownKey, ...]:
    """What the footer of the given screen shows, one tuple per binding.

    An unchanged result means a rebuild would draw the same rows.
    """
    get_key_display = screen.app.get_key_display
    return tuple(
        (
            binding.key,
            get_key_display(binding),
            binding.description,
            enabled,
            tooltip,
        )
        for _, binding, enabled, tooltip in screen.active_bindings.values()
        if binding.show
    )


class Footer(TextualFooter):
    """Key hint bar that skips rebuilds that would change nothing."""

    # What the keys on screen were built from. None before the first
    # build, so that one always reaches the parent and draws the rows.
    drawn_keys: tuple[ShownKey, ...] | None = None

    def compose(self) -> ComposeResult:
        # Record what is drawn AT DRAW TIME, and only when the parent
        # actually draws the keys. Recording in `bindings_changed` instead
        # would go stale when the parent skips the rebuild (it does so
        # while the terminal is unfocused): a change arriving then must
        # keep differing from `drawn_keys`, so that it is drawn on
        # refocus.
        if self._bindings_ready:
            self.drawn_keys = shown_keys(self.screen)
        yield from super().compose()

    def bindings_changed(self, screen: Screen) -> None:
        """Rebuild only when the keys the footer shows have changed."""
        # Only this footer's own screen drives its rows, and only while it
        # is attached, which is exactly where the parent redraws. For any
        # other screen defer to the parent, which turns it into a no-op.
        if (
            self.is_attached
            and screen is self.screen
            and shown_keys(screen) == self.drawn_keys
        ):
            return
        super().bindings_changed(screen)
