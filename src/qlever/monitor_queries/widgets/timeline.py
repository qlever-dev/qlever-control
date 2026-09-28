from __future__ import annotations

from datetime import datetime
from math import ceil

from textual import events
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import Static

from qlever.monitor_queries.models import TimelineBounds
from qlever.monitor_queries.util import format_timestamp

# Width of a full "YYYY-MM-DD HH:MM:SS" stamp, and the gap that
# separates the edge labels from the bar.
STAMP_W = 19
GUTTER = "  "

# Minimum blank columns kept between two adjacent tick labels; the
# actual spacing is this plus the label width (derived, not fixed,
# because multi-day labels are wider than HH:MM ones).
MIN_TICK_GAP = 4

DAY_MS = 24 * 60 * 60 * 1000


def tick_pattern(span_ms: int) -> str:
    """strftime for interior ticks; add the date once the span crosses days.

    Edge labels always carry the full date, so within a single day the
    ticks can stay the compact HH:MM form.
    """
    if span_ms >= DAY_MS:
        return "%m-%d %H:%M"
    return "%H:%M"


def format_tick(ms: int, pattern: str) -> str:
    """Render an epoch (ms) using a tick pattern from `tick_pattern`."""
    return datetime.fromtimestamp(ms / 1000).strftime(pattern)


def span_fraction(ts_ms: int, bounds: TimelineBounds) -> float:
    """Where `ts_ms` sits in the log span, as a 0..1 fraction.

    A zero-length span collapses everything to the start.
    """
    span = bounds.log_end_ms - bounds.log_start_ms
    if span <= 0:
        return 0.0
    return (ts_ms - bounds.log_start_ms) / span


def fraction_to_column(fraction: float, bar_width: int) -> int:
    """Map a 0..1 position along the log span to a bar column index."""
    clamped = min(max(fraction, 0.0), 1.0)
    return round(clamped * (bar_width - 1))


def column_to_ms(col: int, bar_width: int, bounds: TimelineBounds) -> int:
    """Epoch (ms) at a bar column; inverse of fraction_to_column.

    Clamped to the log span so an edge click maps to an edge time.
    """
    if bar_width <= 1:
        return bounds.log_start_ms
    fraction = min(max(col / (bar_width - 1), 0.0), 1.0)
    span = bounds.log_end_ms - bounds.log_start_ms
    return bounds.log_start_ms + round(fraction * span)


def window_width_cells(bounds: TimelineBounds, bar_width: int) -> int:
    """Cells the window block should occupy, based on its duration.

    Width depends only on how long the window is relative to the log
    span, never on where it sits. Floors at one cell so a tiny window
    is still visible.
    """
    log_span = bounds.log_end_ms - bounds.log_start_ms
    if log_span <= 0:
        return 1
    window_span = bounds.window_end_ms - bounds.window_start_ms
    fraction = window_span / log_span
    return max(1, ceil(fraction * bar_width))


def render_track(bounds: TimelineBounds, bar_width: int) -> str:
    """The bar line: full-span track with the window span marked.

    `████` marks the window; `├`/`┤` cap the log edges, but only when
    the window does not already sit on that edge (the window block
    is its own edge marker).
    """
    chars = ["─"] * bar_width
    width = window_width_cells(bounds, bar_width)
    start = fraction_to_column(
        span_fraction(bounds.window_start_ms, bounds), bar_width
    )
    end = start + width - 1
    # Pin the block to the last cell when it overruns the bar, or when
    # the window actually reaches the log end but rounding fell short.
    if end > bar_width - 1 or bounds.window_end_ms >= bounds.log_end_ms:
        end = bar_width - 1
        start = end - width + 1
    for col in range(start, end + 1):
        chars[col] = "█"
    if chars[0] != "█":
        chars[0] = "├"
    if chars[-1] != "█":
        chars[-1] = "┤"
    return "".join(chars)


def render_ticks(bounds: TimelineBounds, bar_width: int) -> str:
    """A blank bar-width line with evenly spaced time labels.

    Tick spacing is derived from the label width so wider multi-day
    labels simply yield fewer ticks instead of overlapping.
    """
    span = bounds.log_end_ms - bounds.log_start_ms
    # Every label uses one pattern, so all labels share a width.
    pattern = tick_pattern(span)
    label_width = len(format_tick(bounds.log_start_ms, pattern))
    # A tick owns its label plus a blank gap; fewer fit when wider.
    step = label_width + MIN_TICK_GAP
    tick_count = max(1, bar_width // step)
    cells = [" "] * bar_width
    for tick_index in range(1, tick_count + 1):
        # Interior positions only; the edges hold the Log start/end labels.
        fraction = tick_index / (tick_count + 1)
        center = fraction_to_column(fraction, bar_width)
        # Time at this position: that same fraction into the log span.
        label = format_tick(
            bounds.log_start_ms + round(fraction * span), pattern
        )
        # Center the label on its column, clamped to stay on the bar.
        start = min(max(center - len(label) // 2, 0), bar_width - len(label))
        for offset, char in enumerate(label):
            cells[start + offset] = char
    return "".join(cells)


class Timeline(Static):
    """Two-row map of the log span with the current window marked.

    Row 1: edge timestamps around a bar whose `████` block is the
    selected window. Row 2: `Log start` / tick labels / `Log end`.
    Geometry is recomputed each paint because the bar width is only
    known after layout.
    """

    can_focus = False

    class Recentered(Message):
        """Posted when the bar is clicked; carries the clicked time."""

        def __init__(self, center_ms: int) -> None:
            super().__init__()
            self.center_ms = center_ms

    bounds = reactive(None, init=False)

    def __init__(self, bounds: TimelineBounds) -> None:
        """Hold the span/window snapshot to draw at render time."""
        super().__init__()
        self.set_reactive(Timeline.bounds, bounds)

    def on_click(self, event: events.Click) -> None:
        """Recenter the window on the clicked bar column.

        Clicks on the edge stamps or gutter (outside the bar) are
        ignored; the screen decides whether `all` makes it a no-op.
        """
        offset = STAMP_W + len(GUTTER)
        bar_width = self.size.width - 2 * offset
        if bar_width < 2:
            return
        col = event.x - offset
        if col < 0 or col >= bar_width:
            return
        self.post_message(
            self.Recentered(column_to_ms(col, bar_width, self.bounds))
        )

    def render(self):
        bar_width = self.size.width - 2 * (STAMP_W + len(GUTTER))
        if bar_width < 2:
            return ""
        top = (
            f"{format_timestamp(self.bounds.log_start_ms)}{GUTTER}"
            f"{render_track(self.bounds, bar_width)}{GUTTER}"
            f"{format_timestamp(self.bounds.log_end_ms)}"
        )
        bottom = (
            f"{'Log start'.ljust(STAMP_W)}{GUTTER}"
            f"{render_ticks(self.bounds, bar_width)}{GUTTER}"
            f"{'Log end'.rjust(STAMP_W)}"
        )
        return f"{top}\n{bottom}"
