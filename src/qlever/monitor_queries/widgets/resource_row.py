"""The Live screen's row of two resource gauges around a server status.

The gauges sit at the edges and the status in the middle, whose width is
pinned so the centered text never jumps as its wording changes.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.reactive import Reactive
from textual.widgets import Static

from qlever.monitor_queries.models import LiveSubtitle, ResourceWindow
from qlever.monitor_queries.widgets.resource_sparkline import ResourceSparkline
from qlever.monitor_queries.widgets.window_stepper import WindowStepper


def format_subtitle(subtitle: LiveSubtitle) -> str:
    """Build the centered two-line subtitle for the resource row.

    Server status on top, a detail below: the active query count, or a
    retry note when the server is unreachable.
    """
    if subtitle.state == "checking":
        status = f"[$warning]Checking[/] server at [b]{subtitle.endpoint}[/]…"
        return f"{status}\n"
    if subtitle.state == "unreachable":
        status = f"[$error]Can't reach server[/] at [b]{subtitle.endpoint}[/]"
        return f"{status}\n[$error]Retrying…[/]"
    status = f"[$success]Server active[/] at [b]{subtitle.endpoint}[/]"
    if subtitle.n_active is None:
        return f"{status}\n"
    return f"{status}\n[b $success]{subtitle.n_active}[/] active queries"


def subtitle_width(endpoint: str) -> int:
    """Visible width of the widest line the subtitle can show for this
    endpoint, so the center can be fixed and never jump or truncate."""
    return len(f"Can't reach server at {endpoint}")


class ResourceRow(Horizontal):
    """A Resource usage and server reachability row under Live view's header.

    Holds a bordered RSS bar gauge on the left, the window stepper over
    server reachability status and number of active queries in the
    middle, and a bordered CPU bar gauge on the right.
    """

    can_focus = False

    subtitle = Reactive(None, init=False)
    window = Reactive(None, init=False)
    stale = Reactive(False, init=False)
    note = Reactive("", init=False)

    def __init__(
        self,
        server_subtitle: LiveSubtitle,
        window: ResourceWindow,
        window_size: str,
        note: str,
    ) -> None:
        super().__init__()
        self.window_size = window_size
        self.set_reactive(ResourceRow.subtitle, server_subtitle)
        self.set_reactive(ResourceRow.window, window)
        self.set_reactive(ResourceRow.note, note)

    def compose(self) -> ComposeResult:
        self.rss_spark = ResourceSparkline(
            self.window, "rss", self.stale, self.note
        )
        yield self.rss_spark
        center = Vertical(
            WindowStepper(
                self.window_size,
                caption="USAGE OVER",
                tooltip=(
                    "How far back the resource usage bars either side "
                    "and the plot below go."
                ),
            ),
            Static(format_subtitle(self.subtitle), classes="resource-status"),
            classes="resource-center",
        )
        center.styles.width = subtitle_width(self.subtitle.endpoint)
        yield center
        self.cpu_spark = ResourceSparkline(
            self.window, "cpu_percent", self.stale, self.note
        )
        yield self.cpu_spark

    def watch_subtitle(self, subtitle: LiveSubtitle) -> None:
        static = self.query_one(".resource-status", Static)
        static.update(format_subtitle(subtitle))

    def watch_window(self, window: ResourceWindow) -> None:
        self.rss_spark.window = window
        self.cpu_spark.window = window

    def watch_note(self, note: str) -> None:
        self.rss_spark.note = note
        self.cpu_spark.note = note

    def watch_stale(self, stale: bool) -> None:
        self.set_class(stale, "stale")
        self.rss_spark.stale = stale
        self.cpu_spark.stale = stale
