"""Tests for the plot widget's scaling, series picking, and colors.

These are the parts a plot definition drives: how far up an axis goes,
which of a plot's columns the window actually has, which color a line
is drawn in, which plots a log can carry, and how wide a stack pads its
labels. Drawing itself is left to the widget.
"""

from qlever.monitor_queries.models import ResourceSeries, ResourceWindow
from qlever.monitor_queries.widgets.resource_plot_pane import (
    PLOTS,
    Plot,
    available_plots,
    axis_top,
    empty_note,
    label_width,
    line_color,
    series_for_keys,
)

NAN = float("nan")


def series(key, values, total=None, axis_min=0.0, axis_max=None):
    """One series with the label and unit the tests never look at."""
    return ResourceSeries(
        key=key,
        label=key,
        unit="u",
        values=values,
        total=total,
        axis_min=axis_min,
        axis_max=axis_max,
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


def test_axis_top_uses_the_capacity_over_the_readings():
    win = window(series("rss", (7.0, 8.0), total=32.9))
    assert axis_top(win, ("rss",)) == 32.9


def test_axis_top_without_a_capacity_uses_the_largest_reading():
    win = window(series("read_bytes_per_s", (1.0, 4.5, 2.0)))
    assert axis_top(win, ("read_bytes_per_s",)) == 4.5


def test_axis_top_skips_the_buckets_that_reported_nothing():
    win = window(series("read_bytes_per_s", (NAN, 3.0, NAN)))
    assert axis_top(win, ("read_bytes_per_s",)) == 3.0


def test_axis_top_of_a_column_that_never_reported_is_zero():
    win = window(series("read_bytes_per_s", (NAN, NAN)))
    assert axis_top(win, ("read_bytes_per_s",)) == 0.0


def test_axis_top_never_drops_below_the_axis_min():
    # A small reading must look small, so the axis keeps its minimum.
    win = window(series("io_stall_percent", (1.0, 3.5), axis_min=20.0))
    assert axis_top(win, ("io_stall_percent",)) == 20.0


def test_axis_top_grows_past_the_axis_min_with_the_data():
    win = window(series("io_stall_percent", (1.0, 65.0), axis_min=20.0))
    assert axis_top(win, ("io_stall_percent",)) == 65.0


def test_axis_top_never_grows_past_the_axis_max():
    # A spike above the cap runs off the top instead of flattening
    # everything else.
    win = window(
        series("read_bytes_per_s", (150.0, 900.0), axis_min=100.0,
               axis_max=200.0)
    )
    assert axis_top(win, ("read_bytes_per_s",)) == 200.0


def test_axis_top_spans_every_series_on_the_side():
    win = window(
        series("read_bytes_per_s", (1.0, 2.0)),
        series("write_bytes_per_s", (0.5, 6.0)),
    )
    keys = ("read_bytes_per_s", "write_bytes_per_s")
    assert axis_top(win, keys) == 6.0


def test_axis_top_ignores_a_key_the_window_does_not_have():
    win = window(series("read_bytes_per_s", (1.0, 2.0)))
    keys = ("read_bytes_per_s", "io_stall_percent")
    assert axis_top(win, keys) == 2.0


def test_axis_top_of_an_absent_side_is_zero():
    win = window(series("rss", (1.0,), total=32.9))
    assert axis_top(win, ("io_stall_percent",)) == 0.0


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
        Plot(name="A", left=("rss",), right=("cpu_percent",)),
        Plot(
            name="B", left=("read_bytes_per_s", "write_bytes_per_s"), right=()
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


def test_label_width_counts_the_zero_of_a_side_with_no_series():
    win = window(series("rss", (7.0,), total=32.9))
    only_absent = Plot(name="A", left=("io_stall_percent",), right=())
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
