"""A bar gauge whose height and color both scale to a fixed capacity.

textual's Sparkline normalizes bars to the data's own min/max, so the
window's largest reading is always full and red. Here every bar is
value/total instead, so a light load stays short and green and only a
near-capacity load reads tall and red.
"""

from __future__ import annotations

from collections.abc import Iterator

from rich.text import Text
from textual.color import Color
from textual.message import Message
from textual.reactive import Reactive
from textual.widgets import Static

from qlever.monitor_queries.models import ResourceSeries

BARS = "▁▂▃▄▅▆▇█"

# Load-color ramp stops: green (idle) → amber (busy) → red (near full).
GREEN = Color(0, 200, 0)
AMBER = Color(220, 180, 0)
RED = Color(220, 0, 0)


def load_color(ratio: float) -> str:
    """Hex color for a 0..1 load: green to amber by half, then to red."""
    if ratio < 0.5:
        return GREEN.blend(AMBER, ratio * 2).hex
    return AMBER.blend(RED, (ratio - 0.5) * 2).hex


def bucket_max(values: tuple[float, ...], width: int) -> Iterator[float]:
    """Group values into `width` columns, each the max of its slice."""
    step = len(values) / width
    for column in range(width):
        lo, hi = int(column * step), int((column + 1) * step)
        chunk = values[lo:hi] or values[lo : lo + 1] or values[-1:]
        yield max(chunk)


def series_title(series: ResourceSeries, stale: bool) -> str:
    """Border label: name, window, and the latest reading against capacity.

    When stale, no recent sample has arrived, so the bars are frozen old
    history. The value is shown as a dash and the window note says so,
    rather than claiming a live reading over the last 5 minutes.
    """
    capacity = "-" if series.total is None else f"{series.total:.1f}"
    if stale:
        return (
            f"[b]{series.label}[/]: "
            f"- / {capacity} {series.unit} (no recent samples)"
        )
    latest = series.values[-1] if series.values else 0
    return (
        f"[b]{series.label}[/]: "
        f"{latest:.1f} / {capacity} {series.unit} (last 5m)"
    )


class ResourceSparkline(Static):
    """One bordered bar gauge for a ResourceSeries.

    Bar height and color both come from value/total, drawn over the two
    content rows under a blank top border that carries the label.
    """

    can_focus = False

    class Clicked(Message):
        """Posted when the gauge is clicked, to open the plot modal."""

    series = Reactive(None, init=False)
    stale = Reactive(False, init=False)

    def __init__(self, series: ResourceSeries, stale: bool) -> None:
        super().__init__()
        self.set_reactive(ResourceSparkline.series, series)
        self.set_reactive(ResourceSparkline.stale, stale)
        self.border_title = series_title(series, stale)

    def watch_series(self, series: ResourceSeries) -> None:
        self.border_title = series_title(series, self.stale)

    def watch_stale(self, stale: bool) -> None:
        self.border_title = series_title(self.series, stale)

    def on_click(self) -> None:
        self.post_message(self.Clicked())

    def render(self) -> Text:
        width, height = self.size.width, self.size.height
        total = self.series.total
        if width < 1 or height < 1 or not total or not self.series.values:
            return Text()
        # One (height, color) per column, both from the curved load.
        cells = []
        for value in bucket_max(self.series.values, width):
            load = min(1.0, max(0.0, value / total))
            # The curve expands the low band the process actually uses and
            # compresses the top it never reaches
            scaled = load**0.7
            cells.append((int(scaled * (8 * height - 1)), load_color(scaled)))
        # Draw top row down to bottom; each row shows its slice of the bar.
        lines = []
        for row in reversed(range(height)):
            low, high = row * 8, (row + 1) * 8
            line = Text()
            for index, color in cells:
                if index < low:
                    line.append(" ")
                elif index >= high:
                    line.append("█", style=color)
                else:
                    line.append(BARS[index % 8], style=color)
            lines.append(line)
        return Text("\n").join(lines)
