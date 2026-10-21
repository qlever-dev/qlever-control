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
from math import ceil, floor, isnan, log10
from typing import NamedTuple

from textual.reactive import Reactive
from textual_plotext import PlotextPlot

from qlever.monitor_queries.models import (
    ResourceEvent,
    ResourceSeries,
    ResourceWindow,
)
from qlever.monitor_queries.resource_data import OPERATION_KEYS
from qlever.monitor_queries.resource_reader import REQUIRED_COLUMNS
from qlever.monitor_queries.util import format_clock

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


# Columns taken by the two y-axis label gutters, which the plot area
# does not get.
Y_AXIS_CHROME = 14


# Data rows = pane height minus the two borders, the x-axis label row
# and the top row, which holds the names. Aim for a tick every
# MIN_ROWS_PER_TICK rows, clamped.
Y_PLOT_CHROME = 4
MIN_ROWS_PER_TICK = 2
MIN_Y_TICKS = 2
MAX_Y_TICKS = 8
# Use at least this many ticks whenever the pane is tall enough.
PREFERRED_MIN_Y_TICKS = 4


def tick_layout(
    height: int, tops: tuple[float, ...], time_labels: bool
) -> tuple[int, int]:
    """Pick the y-tick count and interior row gaps for a pane this tall.

    Returns (count, gaps), shared by both axes so ticks line up. Of the
    counts that fit, picks the one whose worse axis leaves the least
    empty space above its top, and the most ticks on a tie. Counts below
    `PREFERRED_MIN_Y_TICKS` are tried only when it does not fit. Without
    `time_labels`, the row that would show the times holds data instead.
    """
    chrome = Y_PLOT_CHROME if time_labels else Y_PLOT_CHROME - 1
    gaps = max(1, height - chrome - 1)
    if gaps // (PREFERRED_MIN_Y_TICKS - 1) >= MIN_ROWS_PER_TICK:
        fewest = PREFERRED_MIN_Y_TICKS
    else:
        fewest = MIN_Y_TICKS
    count = MIN_Y_TICKS
    least_waste = float("inf")
    for candidate in range(fewest, MAX_Y_TICKS + 1):
        if gaps // (candidate - 1) < MIN_ROWS_PER_TICK:
            break
        # How far the axis reaches past its top, where 1.0 means not at
        # all. A top of zero has no axis to draw.
        waste = max(
            (
                axis_ticks(top, candidate, gaps)[0] / top
                for top in tops
                if top > 0
            ),
            default=1.0,
        )
        if waste <= least_waste:
            count, least_waste = candidate, waste
    return count, gaps


# The steps an axis may count up by, times a power of ten, so its labels
# are round numbers that are easy to read.
ROUND_STEPS = (1, 2, 2.5, 3, 4, 5, 6, 8)


def round_step(least: float) -> float:
    """The smallest step in `ROUND_STEPS` that is at least `least`.

    Only whole steps are picked, so no label shows a fraction. That
    leaves out 2.5 itself, and makes 1 the smallest step.
    """
    power = 10 ** max(0, floor(log10(least)))
    for multiple in ROUND_STEPS:
        step = multiple * power
        if step >= least and step % 1 == 0:
            return step
    return 10 * power


# Every y label is padded to this width, so stacked plots line up in
# time. Five fits the longest short number, like `17.5k`.
Y_LABEL_WIDTH = 5


def short_number(value: float) -> str:
    """A tick value with a `k`, `M`, `G` or `T` suffix, like `17.5k`.

    A tick is a round step times at most seven, so three digits always
    show it in full.
    """
    suffix = ""
    for next_suffix in ("k", "M", "G", "T"):
        if value < 1000:
            break
        value /= 1000
        suffix = next_suffix
    return f"{value:.3g}{suffix}"


def axis_ticks(
    top: float, count: int, gaps: int
) -> tuple[float, list[float], list[str]]:
    """Ticks that count up from 0 by a round step to at least `top`.

    Each step spans the same number of rows, so the gaps look equal.
    Returns the axis maximum, the tick positions and their labels.
    """
    if top <= 0:
        return 1.0, [0], ["0"]
    count = max(2, count)
    row_step = gaps // (count - 1)
    value_step = round_step(top / (count - 1))
    axis_max = value_step * gaps / row_step
    positions = [value_step * index for index in range(count)]
    return axis_max, positions, [short_number(pos) for pos in positions]


# The tops an adjustable axis steps through, starting at the plain
# maximum. Each rung leaves out more of the highest readings, so the
# rest of the data fills more of the plot.
TOP_PERCENTILES = (100, 95, 90, 75)


def percentile(values: list[float], percent: float) -> float:
    """The reading `percent` of the way up the sorted values.

    Picks the nearest reading instead of interpolating between two, so
    100 gives the largest one.
    """
    ordered = sorted(values)
    return ordered[round(percent / 100 * (len(ordered) - 1))]


def axis_top(window: ResourceWindow, axis: Axis, step: int = 0) -> float:
    """Highest value one axis has to show, over all its series.

    A column with a capacity uses it, so a light load stays low
    instead of filling the plot. A column without one uses its own
    readings, cut down to the `TOP_PERCENTILES` entry that `step`
    picks. A rung that leaves the whole axis below one unit (most
    readings are zero on an idle disk) uses the rung above it instead,
    so a stepped axis never shows every reading as one line along the
    ceiling. Zero when the window has none of the axis's columns,
    which tells the plot that side has nothing to draw.
    """
    drawn = series_for_keys(window, axis.keys)
    if not drawn:
        return 0.0
    step = step if axis.adjustable else 0
    # The zero check is on the whole axis, so one idle series does not
    # climb to its spike while a busy one still has a top to give.
    for rung in range(step, -1, -1):
        top = axis.min_top
        for series in drawn:
            if series.total is not None:
                top = max(top, series.total)
                continue
            readings = [value for value in series.values if not isnan(value)]
            if not readings:
                continue
            # Each series keeps its own percentile, so the taller line
            # is not pulled down by the shorter one's low readings.
            top = max(top, percentile(readings, TOP_PERCENTILES[rung]))
        # The ticks step by whole units, so a top below one draws the
        # same 0..1 axis as a top of zero.
        if top >= 1:
            break
    return top


def next_step(
    window: ResourceWindow, plot: Plot, step: int, direction: int
) -> int | None:
    """The rung to move to from `step`, or None when there is none.

    `direction` is 1 to raise the top and -1 to lower it. Either way
    it skips the steps that show the same top, so every press changes
    the plot. None on a plot with no adjustable axis, and at either end.
    """
    axis = plot.adjustable_axis
    if axis is None:
        return None
    tops = [
        axis_top(window, axis, rung) for rung in range(len(TOP_PERCENTILES))
    ]
    if direction > 0:
        # The top only falls as the step rises, so equal tops sit side
        # by side. Go to the first step of the run above this one.
        start = tops.index(tops[step])
        if start > 0:
            return tops.index(tops[start - 1])
        # Every step above shows the same top, so go back to step 0,
        # which also hides the peak label.
        return 0 if step > 0 else None
    for wanted in range(step + 1, len(TOP_PERCENTILES)):
        if tops[wanted] < tops[step]:
            return wanted
    return None


DAY_S = 86_400

# The gaps the x-axis may put between its ticks, in seconds, each one a
# span people count in.
CLOCK_STEPS_S = (
    # 1, 2, 3, 5, 10, 15 and 30 minutes
    60,
    120,
    180,
    300,
    600,
    900,
    1_800,
    # 1, 2, 3, 6 and 12 hours
    3_600,
    7_200,
    10_800,
    21_600,
    43_200,
    DAY_S,
)

# The most gaps the x-axis splits a window into.
MAX_CLOCK_GAPS = 8


def clock_step(span_s: float) -> int:
    """The gap between ticks for a window this long.

    The smallest step in `CLOCK_STEPS_S` with at most `MAX_CLOCK_GAPS`
    gaps, preferring one that divides the window, so every gap is the
    same. A window too long even for daily ticks steps by whole days.
    """
    fitting = [
        step for step in CLOCK_STEPS_S if span_s / step <= MAX_CLOCK_GAPS
    ]
    if not fitting:
        return DAY_S * ceil(span_s / (MAX_CLOCK_GAPS * DAY_S))
    even = [step for step in fitting if span_s % step == 0]
    return (even or fitting)[0]


def clock_ticks(start_s: float, end_s: float) -> tuple[list[float], list[str]]:
    """Ticks in equal steps back from the window's end, with labels.

    Each label is the local minute its tick falls in. The leftover at
    the start gets a tick only if it spans half a step and its minute
    differs from the next tick's. Windows of a day or more show dates.
    """
    if end_s <= start_s:
        return [start_s], [format_clock(start_s * 1000, with_seconds=False)]
    step = clock_step(end_s - start_s)
    positions = []
    tick_s = end_s
    while tick_s >= start_s:
        positions.append(tick_s)
        tick_s -= step
    if (
        positions[-1] - start_s >= step / 2
        and start_s // 60 != positions[-1] // 60
    ):
        positions.append(start_s)
    positions.reverse()
    show_dates = end_s - start_s >= DAY_S
    labels = []
    previous_day = None
    for index, position in enumerate(positions):
        moment = datetime.fromtimestamp(position)
        is_edge = index in (0, len(positions) - 1)
        with_date = show_dates and (is_edge or moment.date() != previous_day)
        labels.append(
            format_clock(
                position * 1000, with_date=with_date, with_seconds=False
            )
        )
        previous_day = moment.date()
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


def clamp(
    values: tuple[float, ...], ceiling: float | None
) -> tuple[float, ...]:
    """Pull the readings above the ceiling down onto it.

    plotext draws nothing at all for a point above the axis, which
    would look like the gap left by a restart. A flat line along the
    ceiling says the reading ran past it instead. The buckets that
    reported nothing stay empty, and a side with no axis of its own
    has no ceiling to pull to.
    """
    if ceiling is None:
        return values
    return tuple(
        value if isnan(value) or value <= ceiling else ceiling
        for value in values
    )


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


class Axis(NamedTuple):
    """One y-axis of a plot: the columns on it share a unit.

    The axis scales to its readings, but never below `min_top`. An
    `adjustable` axis also lets the reader step its top down through
    `TOP_PERCENTILES`, so one spike stops squashing the rest of it.
    """

    keys: tuple[str, ...]
    min_top: float = 0.0
    adjustable: bool = False


class Plot(NamedTuple):
    """What one plot draws: a name, and an axis down each side.

    `left` and `right` are drawn against the left and right y-axis.
    """

    name: str
    left: Axis
    right: Axis

    @property
    def adjustable_axis(self) -> Axis | None:
        """The side that lets the reader step its top, if there is one.

        One side at most: the reader steps a single top, so two
        adjustable sides would move together under one control.
        """
        for axis in (self.left, self.right):
            if axis.adjustable:
                return axis
        return None


# One row per plot, in the order they are offered.
PLOTS = (
    Plot(
        name="Memory and CPU",
        left=Axis(keys=("rss",)),
        right=Axis(keys=("cpu_percent",)),
    ),
    Plot(
        name="Disk I/O",
        # Rates have no capacity to hold the axis steady, so one burst
        # can leave every other reading flat along the bottom.
        left=Axis(
            keys=("read_bytes_per_s", "write_bytes_per_s"), adjustable=True
        ),
        # A stall under a fifth of the time is routine on a busy
        # server. Without the bound the axis would magnify a 2% stall
        # into a plot full of spikes.
        right=Axis(keys=("io_stall_percent",), min_top=20.0),
    ),
    Plot(
        name="Operation time",
        # Times have no capacity to hold the axis steady, so one slow
        # operation can leave every other reading flat along the bottom.
        left=Axis(keys=("query_mean_ms", "update_mean_ms"), adjustable=True),
        right=Axis(keys=("cpu_percent",)),
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


def series_available(
    key: str, has_new_columns: bool, has_operation_types: bool
) -> bool:
    """Whether the logs currently carry the series under this key.

    A required column is in every resource log. An operation series
    needs the metrics log to record the type. Anything else is an
    optional resource column, which only the newer format carries.
    """
    if key in OPERATION_KEYS:
        return has_operation_types
    if key in REQUIRED_COLUMNS:
        return True
    return has_new_columns


def available_plots(
    has_new_columns: bool, has_operation_types: bool
) -> list[Plot]:
    """The plots the two logs can currently fill.

    A plot whose series no log can supply has nothing to draw from and
    is not offered at all. Whether a machine or a window actually
    reported a series is a separate question, answered in the plot
    itself.
    """
    return [
        plot
        for plot in PLOTS
        if all(
            series_available(key, has_new_columns, has_operation_types)
            for key in plot.left.keys + plot.right.keys
        )
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

    window = Reactive(None, init=False)
    plot = Reactive(None, init=False)
    # Each plot's axis top step into `TOP_PERCENTILES`, by plot name,
    # so each plot keeps its own. A missing plot is at step 0.
    top_steps = Reactive(None, init=False)

    def __init__(
        self,
        window: ResourceWindow,
        plot: Plot,
        top_steps: dict[str, int] | None = None,
        time_labels: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        # Draw no background of its own, so the plot sits on the page
        # rather than in a lighter block. The line colors are passed to
        # each plot call, so this theme's own colors never apply.
        self.theme = "textual-clear"
        self.set_reactive(ResourcePlotPane.window, window)
        self.set_reactive(ResourcePlotPane.plot, plot)
        self.set_reactive(ResourcePlotPane.top_steps, dict(top_steps or {}))
        # Stacked plots share one clock row, printed under the last of
        # them, so the ones above give their row back to the data.
        self.time_labels = time_labels

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

    def watch_top_steps(self) -> None:
        """Redraw the same plot against the axis top just stepped to."""
        self.replot()

    @property
    def top_step(self) -> int:
        """The axis top step of the plot being shown."""
        return self.top_steps.get(self.plot.name, 0)

    def step_top(self, direction: int) -> None:
        """Raise (1) or lower (-1) the shown plot's axis top.

        The ends hold instead of wrapping, so the plot stays where it
        is put.
        """
        wanted = next_step(self.window, self.plot, self.top_step, direction)
        if wanted is not None:
            # A new dict, since the pane only redraws when one is assigned.
            self.top_steps = {**self.top_steps, self.plot.name: wanted}

    def on_resize(self) -> None:
        """Redraw at the new size."""
        self.replot()

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
        self.draw_series(window, plot, left_axis_max, right_axis_max)
        self.refresh()

    def draw_axes(
        self, window: ResourceWindow, plot: Plot
    ) -> tuple[float, float | None]:
        """Scale and label both y-axes and the x-axis for this window.

        Each axis reaches one row above its top, which keeps the names
        row clear of data. Returns the axis maximums the names sit at,
        the right one None when it has nothing to read. Labels are
        padded to `Y_LABEL_WIDTH` on the side away from their axis line.
        """
        plt = self.plt
        left_top = axis_top(window, plot.left, self.top_step)
        right_top = axis_top(window, plot.right, self.top_step)
        count, gaps = tick_layout(
            self.size.height, (left_top, right_top), self.time_labels
        )
        left_axis_max, left_positions, left_labels = axis_ticks(
            left_top, count, gaps
        )
        left_axis_max = left_axis_max * (gaps + 1) / gaps
        plt.ylim(0, left_axis_max, yside="left")
        plt.yticks(
            left_positions,
            [label.rjust(Y_LABEL_WIDTH) for label in left_labels],
            yside="left",
        )
        right_axis_max = None
        if right_top > 0:
            right_axis_max, right_positions, right_labels = axis_ticks(
                right_top, count, gaps
            )
            right_axis_max = right_axis_max * (gaps + 1) / gaps
            plt.ylim(0, right_axis_max, yside="right")
            plt.yticks(
                right_positions,
                [label.ljust(Y_LABEL_WIDTH) for label in right_labels],
                yside="right",
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
            axis_label(series)
            for series in series_for_keys(window, plot.left.keys)
        ]
        right_labels = (
            [
                axis_label(series)
                for series in series_for_keys(window, plot.right.keys)
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
        # A stepped axis leaves its tallest readings clamped onto the
        # top, so say what they really reached. It shares the label
        # row, which is already spent, rather than taking a data row.
        if plot.left.adjustable and self.top_step > 0:
            chip = f" ⇡ peak {short_number(axis_top(window, plot.left))} "
            plt.text(
                chip,
                window.start_s + next_col * seconds_per_col,
                left_axis_max,
                yside="left",
                background="default",
                style="inverted",
                alignment="left",
            )
            next_col += len(chip) + LABEL_GAP
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

    def ceiling(self, axis_max: float | None) -> float | None:
        """The value a reading above the axis is pulled down to.

        The top data row, one row under the names. None when the side
        has no axis, so nothing is pulled.
        """
        if axis_max is None:
            return None
        # The row count does not depend on the tops.
        _, gaps = tick_layout(self.size.height, (), self.time_labels)
        return axis_max * gaps / (gaps + 1)

    def draw_series(
        self,
        window: ResourceWindow,
        plot: Plot,
        left_axis_max: float,
        right_axis_max: float | None,
    ) -> None:
        """Plot this plot's lines, or a note when it has none to draw.

        A reading above its axis is clamped onto the row under the top,
        so an axis stepped down past a spike shows a flat line rather
        than a hole. The lines are broken across each restart's
        downtime. Vlines mark the server going down and coming back,
        and an index rebuild starting and ending.
        """
        dark = self.app.current_theme.dark
        plt = self.plt
        # The index is the series' place on its side, which picks its
        # color, so it is taken before the empty ones are dropped.
        lines = [
            (side, index, series)
            for side, keys in (
                ("left", plot.left.keys),
                ("right", plot.right.keys),
            )
            for index, series in enumerate(series_for_keys(window, keys))
            if series.values
        ]
        ceilings = {
            "left": self.ceiling(left_axis_max),
            "right": self.ceiling(right_axis_max),
        }
        for side, index, series in lines:
            times, values = break_at_restarts(
                window.times_s,
                clamp(series.values, ceilings[side]),
                window.events,
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
