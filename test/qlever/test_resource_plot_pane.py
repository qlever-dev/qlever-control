"""Tests for the plot widget's scaling, series picking, and colors.

These are the parts a plot definition drives: how far up an axis goes,
which of a plot's columns the window actually has, which color a line
is drawn in, which plots a log can carry, and how wide a stack pads its
labels. Drawing itself is left to the widget.
"""

from math import isnan

from qlever.monitor_queries.models import ResourceSeries, ResourceWindow
from qlever.monitor_queries.widgets.resource_plot_pane import (
    PLOTS,
    Axis,
    Plot,
    available_plots,
    axis_top,
    clamp,
    empty_note,
    label_width,
    line_color,
    percentile,
    series_for_keys,
)

NAN = float("nan")


def series(key, values, total=None):
    """One series with the label and unit the tests never look at."""
    return ResourceSeries(
        key=key, label=key, unit="u", values=values, total=total
    )


def window(*all_series):
    """A window over ten seconds holding the given series."""
    return ResourceWindow(
        start_s=0.0,
        end_s=10.0,
        times_s=tuple(float(step) for step in range(10)),
        series={one.key: one for one in all_series},
        events=(),
    )


def axis(*keys, min_top=0.0, adjustable=False):
    """An axis over the given columns, unbounded unless asked."""
    return Axis(keys=keys, min_top=min_top, adjustable=adjustable)


def test_axis_top_uses_the_capacity_over_the_readings():
    win = window(series("rss", (7.0, 8.0), total=32.9))
    assert axis_top(win, axis("rss")) == 32.9


def test_axis_top_without_a_capacity_uses_the_largest_reading():
    win = window(series("read_bytes_per_s", (1.0, 4.5, 2.0)))
    assert axis_top(win, axis("read_bytes_per_s")) == 4.5


def test_axis_top_skips_the_buckets_that_reported_nothing():
    win = window(series("read_bytes_per_s", (NAN, 3.0, NAN)))
    assert axis_top(win, axis("read_bytes_per_s")) == 3.0


def test_axis_top_of_a_column_that_never_reported_is_zero():
    win = window(series("read_bytes_per_s", (NAN, NAN)))
    assert axis_top(win, axis("read_bytes_per_s")) == 0.0


def test_axis_top_spans_every_series_on_the_side():
    win = window(
        series("read_bytes_per_s", (1.0, 2.0)),
        series("write_bytes_per_s", (0.5, 6.0)),
    )
    both = axis("read_bytes_per_s", "write_bytes_per_s")
    assert axis_top(win, both) == 6.0


def test_axis_top_ignores_a_key_the_window_does_not_have():
    win = window(series("read_bytes_per_s", (1.0, 2.0)))
    both = axis("read_bytes_per_s", "io_stall_percent")
    assert axis_top(win, both) == 2.0


def test_axis_top_of_an_absent_side_is_zero():
    win = window(series("rss", (1.0,), total=32.9))
    assert axis_top(win, axis("io_stall_percent")) == 0.0


def test_axis_top_never_scales_below_the_min_top():
    win = window(series("io_stall_percent", (1.0, 2.0)))
    bounded = axis("io_stall_percent", min_top=20.0)
    assert axis_top(win, bounded) == 20.0


def test_axis_top_grows_past_the_min_top_with_the_readings():
    win = window(series("io_stall_percent", (1.0, 65.0)))
    bounded = axis("io_stall_percent", min_top=20.0)
    assert axis_top(win, bounded) == 65.0


def test_axis_top_of_an_absent_side_ignores_the_min_top():
    # The zero is what tells the plot this side has nothing to draw, so
    # a bound must not raise an axis for a column the log never has.
    win = window(series("rss", (1.0,), total=32.9))
    bounded = axis("io_stall_percent", min_top=20.0)
    assert axis_top(win, bounded) == 0.0


def test_the_disk_plot_bounds_a_small_io_stall():
    win = window(
        series("read_bytes_per_s", (1.0,)),
        series("io_stall_percent", (2.0,)),
    )
    assert axis_top(win, PLOTS[1].right) == 20.0


def test_only_the_disk_plot_offers_the_step():
    assert PLOTS[1].adjustable
    assert not PLOTS[0].adjustable


def test_percentile_of_a_hundred_is_the_largest_reading():
    assert percentile([3.0, 1.0, 2.0], 100) == 3.0


def test_percentile_leaves_out_the_readings_above_it():
    assert percentile([float(step) for step in range(1, 11)], 75) == 8.0


def test_percentile_of_one_reading_is_that_reading():
    assert percentile([4.0], 90) == 4.0


def test_axis_top_steps_down_past_a_spike():
    readings = tuple(float(step) for step in range(1, 10)) + (100.0,)
    win = window(series("read_bytes_per_s", readings))
    stepped = axis("read_bytes_per_s", adjustable=True)
    assert axis_top(win, stepped) == 100.0
    assert axis_top(win, stepped, step=2) == 9.0


def test_axis_top_ignores_the_step_on_a_fixed_axis():
    readings = tuple(float(step) for step in range(1, 10)) + (100.0,)
    win = window(series("read_bytes_per_s", readings))
    assert axis_top(win, axis("read_bytes_per_s"), step=3) == 100.0


def test_axis_top_gives_each_series_its_own_percentile():
    # Pooling the two would answer 80, because the low write readings
    # push the taller read line's own cut-off down the sorted list.
    win = window(
        series(
            "read_bytes_per_s",
            tuple(float(step * 10) for step in range(1, 11)),
        ),
        series("write_bytes_per_s", (1.0,) * 10),
    )
    both = axis("read_bytes_per_s", "write_bytes_per_s", adjustable=True)
    assert axis_top(win, both, step=2) == 90.0


def test_the_disk_plot_steps_its_rate_axis():
    readings = tuple(float(step) for step in range(1, 10)) + (100.0,)
    win = window(series("read_bytes_per_s", readings))
    assert axis_top(win, PLOTS[1].left, step=2) == 9.0


def test_clamp_pulls_a_reading_above_the_ceiling_onto_it():
    assert clamp((1.0, 9.0, 2.0), 5.0) == (1.0, 5.0, 2.0)


def test_clamp_leaves_the_readings_under_the_ceiling_alone():
    assert clamp((1.0, 5.0), 5.0) == (1.0, 5.0)


def test_clamp_keeps_the_buckets_that_reported_nothing_empty():
    # A gap means the server was down, so clamping must not fill it.
    clamped = clamp((NAN, 9.0), 5.0)
    assert isnan(clamped[0])
    assert clamped[1] == 5.0


def test_clamp_without_a_ceiling_changes_nothing():
    assert clamp((1.0, 9.0), None) == (1.0, 9.0)


def test_series_for_keys_keeps_the_plot_order():
    win = window(
        series("write_bytes_per_s", (1.0,)),
        series("read_bytes_per_s", (2.0,)),
    )
    keys = ("read_bytes_per_s", "write_bytes_per_s")
    assert [one.key for one in series_for_keys(win, keys)] == list(keys)


def test_series_for_keys_drops_the_absent_ones():
    win = window(series("read_bytes_per_s", (1.0,)))
    keys = ("read_bytes_per_s", "write_bytes_per_s")
    found = series_for_keys(win, keys)
    assert [one.key for one in found] == ["read_bytes_per_s"]


def test_series_for_keys_of_an_empty_window_is_empty():
    assert series_for_keys(window(), ("rss",)) == []


def test_an_old_log_offers_only_the_plot_it_has_columns_for():
    offered = available_plots(log_has_new_columns=False)
    assert [plot.name for plot in offered] == ["Memory and CPU"]


def test_a_new_log_offers_every_plot():
    assert available_plots(log_has_new_columns=True) == list(PLOTS)


def test_an_empty_window_says_it_has_no_samples():
    win = ResourceWindow(
        start_s=0.0, end_s=10.0, times_s=(), series={}, events=()
    )
    assert empty_note(win, PLOTS[0]) == "No samples in this window"


def test_a_window_missing_only_this_plot_names_the_plot():
    win = window(series("rss", (1.0,), total=32.9))
    assert empty_note(win, PLOTS[1]) == "No Disk I/O readings in this window"


def test_label_width_takes_the_longest_number_in_the_stack():
    win = window(
        series("rss", (7.0,), total=32.9),
        series("cpu_percent", (400.0,), total=16.0),
        series("read_bytes_per_s", (2.0,)),
        series("write_bytes_per_s", (6000.0,)),
    )
    stack = [
        Plot(name="A", left=axis("rss"), right=axis("cpu_percent")),
        Plot(
            name="B",
            left=axis("read_bytes_per_s", "write_bytes_per_s"),
            right=axis(),
        ),
    ]
    # 33, 16 and 2 are two digits or fewer; 6000 is four.
    assert label_width(win, stack) == 4


def test_label_width_of_one_plot_is_its_own_longest():
    win = window(
        series("rss", (7.0,), total=32.9),
        series("cpu_percent", (400.0,), total=16.0),
    )
    assert label_width(win, [PLOTS[0]]) == 2


def test_label_width_follows_the_axis_down_a_step():
    readings = tuple(float(value) for value in range(1, 10)) + (100.0,)
    win = window(series("read_bytes_per_s", readings))
    stepped = [
        Plot(
            name="A",
            left=axis("read_bytes_per_s", adjustable=True),
            right=axis(),
        )
    ]
    assert label_width(win, stepped) == 3
    assert label_width(win, stepped, step=2) == 1


def test_label_width_counts_the_zero_of_a_side_with_no_series():
    win = window(series("rss", (7.0,), total=32.9))
    only_absent = Plot(name="A", left=axis("io_stall_percent"), right=axis())
    assert label_width(win, [only_absent]) == 1


def test_label_width_of_no_plots_pads_nothing():
    assert label_width(window(), []) == 0


def test_left_axis_gives_each_series_its_own_color():
    first = line_color("left", 0, dark=True)
    second = line_color("left", 1, dark=True)
    assert first != second


def test_right_axis_is_grey_and_not_a_left_hue():
    red, green, blue = line_color("right", 0, dark=True)
    assert red == green == blue
    assert (red, green, blue) != line_color("left", 0, dark=True)


def test_line_colors_differ_between_the_theme_backgrounds():
    assert line_color("left", 0, dark=True) != line_color(
        "left", 0, dark=False
    )
    assert line_color("right", 0, dark=True) != line_color(
        "right", 0, dark=False
    )
