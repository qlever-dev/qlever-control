from __future__ import annotations

from textual.reactive import Reactive
from textual.widgets import Static

from qlever.monitor_queries.models import LiveSubtitle


def format_subtitle(subtitle: LiveSubtitle) -> str:
    """Build the Live subtitle text from current state and server endpoint."""
    if subtitle.state == "checking":
        return f"[$warning]Checking[/] server at [b]{subtitle.endpoint}[/]…"
    if subtitle.state == "unreachable":
        return (
            f"[$error]Can't reach server[/] at [b]{subtitle.endpoint}[/]"
            f" · retrying every 5s"
        )
    if subtitle.n_active is None:
        return f"[$success]Server active[/] at [b]{subtitle.endpoint}[/]"
    return (
        f"[$success]Server active[/] at [b]{subtitle.endpoint}[/]"
        f" · [b $success]{subtitle.n_active}[/] active queries"
    )


class LiveStatusRow(Static):
    """One-line subtitle under the HeaderRow on the Live screen."""

    can_focus = False

    subtitle = Reactive(None, init=False)

    def __init__(self, subtitle: LiveSubtitle) -> None:
        """Render the subtitle text computed from `subtitle`."""
        super().__init__(format_subtitle(subtitle))
        self.set_reactive(LiveStatusRow.subtitle, subtitle)

    def watch_subtitle(self, subtitle: LiveSubtitle) -> None:
        self.update(format_subtitle(subtitle))
