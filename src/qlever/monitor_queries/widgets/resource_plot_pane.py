"""Dual-axis plot of a resource window, shared by the pane and the modal.

Draws the window the screen hands it and owns no data of its own, so the
same widget serves Live's rolling window and Historic's fixed span. A
`Plot` says which columns to draw and on which axis, so one widget draws
every plot.

Axis ticks are picked by hand because plotext's defaults crowd a short
terminal pane.
"""

from __future__ import annotations

from datetime import datetime
from math import isnan
from typing import NamedTuple

from textual.message import Message
from textual.reactive import Reactive
from textual_plotext import PlotextPlot

from qlever.monitor_queries.models import (
    ResourceEvent,
    ResourceSeries,
    ResourceWindow,
)
from qlever.monitor_queries.resource_reader import REQUIRED_COLUMNS

RgbColor = tuple[int, int, int]


class AxisColors(NamedTuple):
    """The colors one axis lends its lines, in the plot's series order."""

    light: tuple[RgbColor, ...]
    dark: tuple[RgbColor, ...]


# Saturated on the left, one hue per series, and grey on the right,
# which never holds more than one. Deeper colors on a light background
# and brighter ones on a dark background, so both stay legible.
AXIS_COLORS = {
    "left": AxisColors(
        light=((176, 25, 127), (0, 150, 130)),
        dark=((255, 105, 190), (34, 211, 200)),
    ),
    "right": AxisColors(light=((100, 100, 100),), dark=((170, 170, 170),)),
}


class EventStyle(NamedTuple):
    """How one kind of event is labelled and coloured."""

    label: str
    light: RgbColor
    dark: RgbColor


# One row per event kind, in the order the tooltip lists them. Orange
# is the server going down and green it coming back; blue is a rebuild
# starting and violet it ending.
EVENT_STYLE = {
    "server_down": EventStyle(
        label="server down", light=(200, 110, 20), dark=(240, 160, 60)
    ),
    "server_up": EventStyle(
        label="server up", light=(30, 140, 70), dark=(90, 210, 130)
    ),
    "rebuild_start": EventStyle(
        label="rebuild start", light=(30, 90, 200), dark=(110, 160, 255)
    ),
    "rebuild_end": EventStyle(
        label="rebuild end", light=(130, 60, 180), dark=(190, 130, 240)
    ),
}


def line_color(side: str, index: int, dark: bool) -> RgbColor:
    """Pick a line's color for the active theme background.

    The color comes from the axis a line is read against, so the same
    series looks the same on every plot it appears on.
    """
    colors = AXIS_COLORS[side]
    return (colors.dark if dark else colors.light)[index]


def event_color(kind: str, dark: bool) -> RgbColor:
    """Pick an event's line color for the active theme background."""
    style = EVENT_STYLE[kind]
    return style.dark if dark else style.light


# A plot column holds 2 braille dots across, so 2 buckets per usable
# column is the most the plot can resolve; more just overplots. Reserve
# columns for the two y-axis label gutters.
Y_AXIS_CHROME = 16
MIN_BUCKETS = 60


def buckets_for_width(width: int) -> int:
    """Buckets worth reading for a pane this wide (2 per braille column)."""
    usable_cols = max(10, width - Y_AXIS_CHROME)
    return max(MIN_BUCKETS, usable_cols * 2)


# Interior rows = pane height minus the two borders and the x-axis label
# row. Aim for a tick every MIN_ROWS_PER_TICK rows, clamped.
Y_PLOT_CHROME = 3
MIN_ROWS_PER_TICK = 2
MIN_Y_TICKS = 2
MAX_Y_TICKS = 8


def tick_layout(height: int, max_ticks: int) -> tuple[int, int]:
    """Pick the y-tick count and interior row gaps for a pane this tall.

    Returns (count, gaps), shared by both axes so ticks line up. Picks the
    most ticks whose leftover rows stay below one gap, so the space above
    the top tick never exceeds a tick interval. max_ticks caps the count
    so the smaller axis keeps distinct labels.
    """
    gaps = max(1, height - Y_PLOT_CHROME - 1)
    cap = min(MAX_Y_TICKS, max_ticks)
    count = MIN_Y_TICKS
    for candidate in range(MIN_Y_TICKS, cap + 1):
        step = gaps // (candidate - 1)
        if step < MIN_ROWS_PER_TICK:
            break
        if gaps - step * (candidate - 1) < step:
            count = candidate
    return count, gaps


def axis_ticks(
    top: float, count: int, gaps: int
) -> tuple[float, list[int], list[str]]:
    """Ticks by a constant integer step, ending near top.

    Labels rise by one whole-number step (0, s, 2s, ...) so the numbers
    are as evenly spaced as the rows. The step is round(top / gaps
    between ticks), so the last label is the closest step multiple to the
    capacity. The axis maximum maps one label step onto the whole-row
    step, keeping every gap equal. Returns (axis_max, positions, labels).
    """
    if top <= 0:
        return 1.0, [0], ["0"]
    count = max(2, count)
    row_step = gaps // (count - 1)
    value_step = max(1, round(top / (count - 1)))
    axis_max = value_step * gaps / row_step
    positions = [value_step * index for index in range(count)]
    return axis_max, positions, [str(pos) for pos in positions]


def axis_top(window: ResourceWindow, keys: tuple[str, ...]) -> float:
    """Highest value one axis has to show, over all its series.

    A column with a capacity is measured against it, so a light load
    stays low instead of filling the plot. A column without a capacity
    is measured against its largest reading, skipping the buckets where
    it reported nothing, clamped into the axis bounds the series
    carries. Zero when the window holds none of the keys.
    """
    top = 0.0
    for key in keys:
        series = window.series.get(key)
        if series is None:
            continue
        if series.total is not None:
            top = max(top, series.total)
        else:
            readings = [value for value in series.values if not isnan(value)]
            series_top = max([series.axis_min, *readings])
            if series.axis_max is not None:
                series_top = min(series_top, series.axis_max)
            top = max(top, series_top)
    return top


def label_width(window: ResourceWindow, plots: list[Plot]) -> int:
    """Digits in the longest y label these plots print for this window.

    plotext sizes its gutters from the longest label it has, so stacked
    plots whose numbers differ in length start their data at different
    columns and the same moment does not line up down the stack.
    Padding every label to this width lines them up.
    """
    width = 0
    for plot in plots:
        for keys in (plot.left, plot.right):
            highest = axis_top(window, keys)
            width = max(width, len(str(round(highest))))
    return width


def clock_ticks(
    start_s: float, end_s: float, count: int = 5
) -> tuple[list[float], list[str]]:
    """Evenly spaced x positions across the window with HH:MM:SS labels.

    Returns the tick positions in epoch seconds and their clock-time
    labels, so the x-axis reads as wall-clock time for both a rolling
    live window and a fixed historic span.
    """
    if end_s <= start_s:
        return [start_s], [
            datetime.fromtimestamp(start_s).strftime("%H:%M:%S")
        ]
    span = end_s - start_s
    positions = [
        start_s + span * index / (count - 1) for index in range(count)
    ]
    labels = [
        datetime.fromtimestamp(position).strftime("%H:%M:%S")
        for position in positions
    ]
    return positions, labels


def break_at_restarts(
    times: tuple[float, ...],
    values: tuple[float, ...],
    events: tuple[ResourceEvent, ...],
) -> tuple[list[float], list[float]]:
    """Insert a gap at each restart so the line is not drawn across it.

    Breaking where the server came back leaves the downtime empty:
    before the first point at or after that time, add a NaN point,
    which plotext leaves unconnected. A restart before the first point
    or after the last adds no gap.
    """
    out_times = []
    out_values = []
    starts = [event.time_s for event in events if event.kind == "server_up"]
    idx = 0
    for time_s, value in zip(times, values):
        while idx < len(starts) and time_s >= starts[idx]:
            if out_times:
                out_times.append(starts[idx])
                out_values.append(float("nan"))
            idx += 1
        out_times.append(time_s)
        out_values.append(value)
    return out_times, out_values


def color_markup(color: RgbColor) -> str:
    """A Rich color tag for an RGB triplet."""
    return "rgb({}, {}, {})".format(*color)


def marker_legend(window: ResourceWindow, dark: bool) -> str:
    """One line per event kind in the window, colored to match its line."""
    present = {event.kind for event in window.events}
    return "\n".join(
        f"[{color_markup(event_color(kind, dark))}]│ {style.label}[/]"
        for kind, style in EVENT_STYLE.items()
        if kind in present
    )


def axis_label(series: ResourceSeries) -> str:
    """An axis name with its unit, like `RSS (GB)`."""
    return f"{series.label} ({series.unit})"


def series_for_keys(
    window: ResourceWindow, keys: tuple[str, ...]
) -> list[ResourceSeries]:
    """The series one axis has, in plot order, skipping the absent keys.

    Both the names and the lines are drawn from this list, so a name
    always takes the color its own line was drawn in.
    """
    return [window.series[key] for key in keys if key in window.series]


class Plot(NamedTuple):
    """What one plot draws: a name, and the columns on each axis.

    `left` and `right` hold log column keys, drawn against the left and
    the right y-axis. A side's keys share a unit, because one axis
    carries one unit label.
    """

    name: str
    left: tuple[str, ...]
    right: tuple[str, ...]


# One row per plot, in the order they are offered.
PLOTS = (
    Plot(name="Memory and CPU", left=("rss",), right=("cpu_percent",)),
    Plot(
        name="Disk I/O",
        left=("read_bytes_per_s", "write_bytes_per_s"),
        right=("io_stall_percent",),
    ),
)


def empty_note(window: ResourceWindow, plot: Plot) -> str:
    """Why a plot drew no lines: no samples at all, or none of its own.

    A machine that does not measure a column reports it blank, so the
    log can carry a plot that this server never fills in.
    """
    if window.times_s:
        return f"No {plot.name} readings in this window"
    return "No samples in this window"


def available_plots(log_has_new_columns: bool) -> list[Plot]:
    """The plots a log of this format can carry.

    An older log holds only the columns every log has, so a plot
    needing any other one has nothing to draw from and is not offered.
    Whether a machine or a window actually reported a column is a
    separate question, answered in the plot itself.
    """
    if log_has_new_columns:
        return list(PLOTS)
    return [
        plot
        for plot in PLOTS
        if all(key in REQUIRED_COLUMNS for key in plot.left + plot.right)
    ]


# The plot name and the axis names share the plot's top row, so the
# name is drawn only when they all fit with a gap between them.
LABEL_GAP = 2


class ResourcePlotPane(PlotextPlot):
    """Dual-axis plot of one resource window.

    Draws the window and the plot the screen hands it, so the same
    widget serves Live's rolling window, Historic's fixed span, and
    every plot in `PLOTS`.
    """

    can_focus = False

    class BucketsChanged(Message):
        """Posted when a resize changes how many buckets the pane fits.

        Bubbles up so whoever holds the readings decides whether they
        are worth re-reading at the new width.
        """

        def __init__(self, buckets: int) -> None:
            super().__init__()
            self.buckets = buckets

    window = Reactive(None, init=False)
    plot = Reactive(None, init=False)

    def __init__(
        self,
        window: ResourceWindow,
        plot: Plot,
        time_labels: bool = True,
        label_width: int = 0,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.set_reactive(ResourcePlotPane.window, window)
        self.set_reactive(ResourcePlotPane.plot, plot)
        # Stacked plots share one clock row, printed under the last of
        # them, so the ones above give their row back to the data.
        self.time_labels = time_labels
        # Width to pad the y labels to, so a stack's gutters come out
        # the same size. Zero for a plot drawn on its own.
        self.label_width = label_width
        self.last_buckets = None

    def on_mount(self) -> None:
        """Draw once, and again whenever the theme's colors change."""
        self.replot()
        self.app.theme_changed_signal.subscribe(
            self, lambda theme: self.replot()
        )

    def watch_window(self) -> None:
        """Redraw with the window the screen just handed down."""
        self.replot()

    def watch_plot(self) -> None:
        """Redraw the same window as the plot it was just switched to."""
        self.replot()

    def on_resize(self) -> None:
        """Redraw at the new size, and say so if the bucket count moved.

        Staying quiet unless the count moved keeps a resize that only
        changed the height from asking for a re-read. A hidden pane has
        width 0 and is skipped.
        """
        self.replot()
        if self.size.width > 0:
            buckets = buckets_for_width(self.size.width)
            if buckets != self.last_buckets:
                self.last_buckets = buckets
                self.post_message(self.BucketsChanged(buckets))

    def replot(self) -> None:
        """Draw the current window, and set the marker tooltip.

        Skipped while hidden; being shown fires a resize that redraws.
        """
        if not self.display:
            return
        window, plot = self.window, self.plot
        legend = marker_legend(window, self.app.current_theme.dark)
        self.tooltip = legend or None
        self.plt.clear_figure()
        self.plt.xlim(window.start_s, window.end_s)
        left_axis_max, right_axis_max = self.draw_axes(window, plot)
        self.draw_labels(window, plot, left_axis_max, right_axis_max)
        self.draw_series(window, plot, left_axis_max)
        self.refresh()

    def padded(self, labels: list[str]) -> list[str]:
        """Right-justify y labels so a stack's gutters match."""
        return [label.rjust(self.label_width) for label in labels]

    def draw_axes(
        self, window: ResourceWindow, plot: Plot
    ) -> tuple[float, float | None]:
        """Scale and label both y-axes and the x-axis for this window.

        Returns the two axis maximums the labels anchor to. The right
        one is None when the window has nothing to read against it, so
        that axis gets no ticks. Labels are padded to `label_width`,
        which lines a stack's gutters up and is zero on its own.
        """
        plt = self.plt
        left_top = axis_top(window, plot.left)
        right_top = axis_top(window, plot.right)
        # Cap the shared tick count by the smaller axis so its labels stay
        # distinct.
        smaller_top = min(left_top, right_top) if right_top > 0 else left_top
        count, gaps = tick_layout(self.size.height, round(smaller_top) + 1)
        left_axis_max, left_positions, left_labels = axis_ticks(
            left_top, count, gaps
        )
        plt.ylim(0, left_axis_max, yside="left")
        plt.yticks(left_positions, self.padded(left_labels), yside="left")
        right_axis_max = None
        if right_top > 0:
            right_axis_max, right_positions, right_labels = axis_ticks(
                right_top, count, gaps
            )
            plt.ylim(0, right_axis_max, yside="right")
            plt.yticks(
                right_positions, self.padded(right_labels), yside="right"
            )
        else:
            plt.ylim(0, None, yside="right")
        if self.time_labels:
            positions, labels = clock_ticks(window.start_s, window.end_s)
            plt.xticks(positions, labels)
        else:
            plt.xticks([], [])
        return left_axis_max, right_axis_max

    def draw_labels(
        self,
        window: ResourceWindow,
        plot: Plot,
        left_axis_max: float,
        right_axis_max: float | None,
    ) -> None:
        """Name each series in its axis corner and the plot between them.

        The series names sit in the top corners, colored to match their
        lines, so the reader maps line to axis without a stacked legend.
        A bottom label row would sit under the footer keys.
        """
        dark = self.app.current_theme.dark
        plt = self.plt
        usable_cols = max(1, self.size.width - Y_AXIS_CHROME)
        left_labels = [
            axis_label(series) for series in series_for_keys(window, plot.left)
        ]
        right_labels = (
            [
                axis_label(series)
                for series in series_for_keys(window, plot.right)
            ]
            if right_axis_max is not None
            else []
        )
        # plotext anchors text at a time, so a name's place in the row
        # is a column count turned back into seconds.
        seconds_per_col = (window.end_s - window.start_s) / usable_cols
        next_col = 0
        for index, label in enumerate(left_labels):
            plt.text(
                label,
                window.start_s + next_col * seconds_per_col,
                left_axis_max,
                yside="left",
                color=line_color("left", index, dark),
                background="default",
                alignment="left",
            )
            next_col += len(label) + LABEL_GAP
        for index, label in enumerate(right_labels):
            plt.text(
                label,
                window.end_s,
                right_axis_max,
                yside="right",
                color=line_color("right", index, dark),
                background="default",
                alignment="right",
            )
        # plotext neither wraps nor clips, so a name that does not fit
        # would be painted over the data.
        # The cursor already counts the left names and one gap after them.
        row_width = (
            next_col
            + len(plot.name)
            + LABEL_GAP
            + sum(len(label) for label in right_labels)
        )
        if row_width <= usable_cols:
            plt.text(
                plot.name,
                (window.start_s + window.end_s) / 2,
                left_axis_max,
                yside="left",
                background="default",
                style="bold",
                alignment="center",
            )

    def draw_series(
        self, window: ResourceWindow, plot: Plot, left_axis_max: float
    ) -> None:
        """Plot this plot's lines, or a note when it has none to draw.

        The lines are broken across each restart's downtime. Vlines mark
        the server going down and coming back, and an index rebuild
        starting and ending.
        """
        dark = self.app.current_theme.dark
        plt = self.plt
        # The index is the series' place on its side, which picks its
        # color, so it is taken before the empty ones are dropped.
        lines = [
            (side, index, series)
            for side, keys in (("left", plot.left), ("right", plot.right))
            for index, series in enumerate(series_for_keys(window, keys))
            if series.values
        ]
        for side, index, series in lines:
            times, values = break_at_restarts(
                window.times_s, series.values, window.events
            )
            plt.plot(
                times,
                values,
                yside=side,
                marker="braille",
                color=line_color(side, index, dark),
            )
        if not lines:
            # plotext only draws a y-axis for a side that has data, so an
            # empty plot would frame the left axis but not the right.
            # Anchor an invisible point on each side to keep both framed.
            plt.plot([window.start_s], [0], yside="left", marker=" ")
            plt.plot([window.start_s], [0], yside="right", marker=" ")
            plt.text(
                empty_note(window, plot),
                (window.start_s + window.end_s) / 2,
                left_axis_max / 2,
                yside="left",
                background="default",
                alignment="center",
            )
        for event in window.events:
            plt.vline(event.time_s, color=event_color(event.kind, dark))
