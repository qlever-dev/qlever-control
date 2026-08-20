"""Dual-axis RSS and CPU plot, shared by the inline pane and the modal."""

from __future__ import annotations

from datetime import datetime

from textual_plotext import PlotextPlot

from qlever.monitor_queries.models import ResourcePlot

# Saturated line colors, one pair per theme background: deeper on a
# light background, brighter on a dark one, so both stay legible.
CPU_COLOR_LIGHT = (0, 150, 130)
RSS_COLOR_LIGHT = (176, 25, 127)
CPU_COLOR_DARK = (34, 211, 200)
RSS_COLOR_DARK = (255, 105, 190)

# Restart markers, one pair per theme like the series colors: orange
# marks the server going down, green it coming back. Kept clear of the
# RSS and CPU line colors and legible on either background.
STOP_COLOR_LIGHT = (200, 110, 20)
START_COLOR_LIGHT = (30, 140, 70)
STOP_COLOR_DARK = (240, 160, 60)
START_COLOR_DARK = (90, 210, 130)


def series_colors(
    dark: bool,
) -> tuple[tuple[int, int, int], tuple[int, int, int]]:
    """Pick the (RSS, CPU) line colors for the active theme background."""
    if dark:
        return RSS_COLOR_DARK, CPU_COLOR_DARK
    return RSS_COLOR_LIGHT, CPU_COLOR_LIGHT


def restart_colors(
    dark: bool,
) -> tuple[tuple[int, int, int], tuple[int, int, int]]:
    """Pick the (stop, start) restart marker colors for the active theme."""
    if dark:
        return STOP_COLOR_DARK, START_COLOR_DARK
    return STOP_COLOR_LIGHT, START_COLOR_LIGHT


# A plot column holds 2 braille dots across, so 2 points per usable
# column is the most the plot can resolve; more just overplots. Reserve
# columns for the two y-axis label gutters.
Y_AXIS_CHROME = 16
MIN_PLOT_POINTS = 60


def point_budget(width: int) -> int:
    """Points worth plotting for a pane this wide (2 per braille column)."""
    usable_cols = max(10, width - Y_AXIS_CHROME)
    return max(MIN_PLOT_POINTS, usable_cols * 2)


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


def break_at_starts(
    times: tuple[float, ...],
    values: tuple[float, ...],
    start_times: tuple[float, ...],
) -> tuple[list[float], list[float]]:
    """Insert a gap at each restart so the line is not drawn across it.

    Breaking at the start time leaves the downtime, from the stop to the
    start, empty: before the first point at or after a start time, add a
    NaN point at that time; plotext leaves a NaN unconnected. A start
    before the first point or after the last adds no gap.
    """
    out_times = []
    out_values = []
    starts = list(start_times)
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


class ResourcePlotPane(PlotextPlot):
    """Dual-axis RSS and CPU plot over a time window."""

    can_focus = False

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.data = None

    def draw(self, data: ResourcePlot) -> None:
        """Draw new data, and keep it for redraws at a new size."""
        self.data = data
        self.replot()

    def replot(self) -> None:
        """Draw the current window: SS on the left axis, CPU on the right.

        Frames the window and axes, then plots the two series or, when the
        window holds no samples, a centered note. Restarts show as an
        orange stop line and a green start line, with the series broken
        across the downtime between them.
        """
        if self.data is None:
            return
        data = self.data
        self.plt.clear_figure()
        self.plt.xlim(data.start_s, data.end_s)
        rss_max, cpu_max = self.draw_axes(data)
        self.draw_labels(data, rss_max, cpu_max)
        self.draw_series(data, rss_max)
        self.refresh()

    def draw_axes(self, data: ResourcePlot) -> tuple[float, float | None]:
        """Scale and label both y-axes and the x-axis for this window.

        Returns (rss_max, cpu_max), the axis tops the labels anchor to;
        cpu_max is None when the core count is unknown, so the right axis
        gets no ticks.
        """
        plt = self.plt
        # Base the right axis on the tallest CPU point when the core count
        # is unknown.
        cpu_top = (
            data.cpu_total
            if data.cpu_total is not None
            else max(data.cpu_cores, default=0)
        )
        # Cap the shared tick count by the smaller axis so its labels stay
        # distinct.
        smaller_top = (
            min(data.rss_total, cpu_top) if cpu_top > 0 else data.rss_total
        )
        count, gaps = tick_layout(self.size.height, round(smaller_top) + 1)
        rss_max, rss_positions, rss_labels = axis_ticks(
            data.rss_total, count, gaps
        )
        plt.ylim(0, rss_max, yside="left")
        plt.yticks(rss_positions, rss_labels, yside="left")
        cpu_max = None
        if cpu_top > 0:
            cpu_max, cpu_positions, cpu_labels = axis_ticks(
                cpu_top, count, gaps
            )
            plt.ylim(0, cpu_max, yside="right")
            plt.yticks(cpu_positions, cpu_labels, yside="right")
        else:
            plt.ylim(0, data.cpu_total, yside="right")
        positions, labels = clock_ticks(data.start_s, data.end_s)
        plt.xticks(positions, labels)
        return rss_max, cpu_max

    def draw_labels(
        self, data: ResourcePlot, rss_max: float, cpu_max: float | None
    ) -> None:
        """Name each series in its axis corner.

        The series names sit in the top corners, colored to match their
        lines, so the reader maps line to axis without a stacked legend.
        A bottom label row would sit under the footer keys.
        """
        dark = self.app.current_theme.dark
        rss_color, cpu_color = series_colors(dark)
        plt = self.plt
        plt.text(
            "RSS (GB)",
            data.start_s,
            rss_max,
            yside="left",
            color=rss_color,
            background="default",
            alignment="left",
        )
        if cpu_max is not None:
            plt.text(
                "CPU (cores)",
                data.end_s,
                cpu_max,
                yside="right",
                color=cpu_color,
                background="default",
                alignment="right",
            )

    def draw_series(self, data: ResourcePlot, rss_max: float) -> None:
        """Plot the RSS and CPU lines, or a note when the window is empty.

        The lines are broken across each restart's downtime. An orange
        vline marks each stop and a green one each start.
        """
        dark = self.app.current_theme.dark
        rss_color, cpu_color = series_colors(dark)
        stop_color, start_color = restart_colors(dark)
        plt = self.plt
        if data.times_s:
            rss_times, rss_values = break_at_starts(
                data.times_s, data.rss_gb, data.start_times_s
            )
            cpu_times, cpu_values = break_at_starts(
                data.times_s, data.cpu_cores, data.start_times_s
            )
            plt.plot(
                rss_times,
                rss_values,
                yside="left",
                marker="braille",
                color=rss_color,
            )
            plt.plot(
                cpu_times,
                cpu_values,
                yside="right",
                marker="braille",
                color=cpu_color,
            )
        else:
            # plotext only draws a y-axis for a side that has data, so an
            # empty window would show the RSS axis but not the CPU one.
            # Anchor an invisible point on each side to keep both framed.
            plt.plot([data.start_s], [0], yside="left", marker=" ")
            plt.plot([data.start_s], [0], yside="right", marker=" ")
            plt.text(
                "No samples in this window",
                (data.start_s + data.end_s) / 2,
                rss_max / 2,
                yside="left",
                background="default",
                alignment="center",
            )
        for stop_s in data.stop_times_s:
            plt.vline(stop_s, color=stop_color)
        for start_s in data.start_times_s:
            plt.vline(start_s, color=start_color)
