"""Tests for the resource data layer: parse, tail, seek, windowed read, plot."""

import io
from dataclasses import replace

import pytest

from qlever.monitor_queries.models import ResourceSample, ResourceTotals
from qlever.monitor_queries.resource_data import (
    LOG_COLUMNS,
    OPTIONAL_COLUMNS,
    REQUIRED_COLUMNS,
    SEEK_BACKUP_BYTES,
    get_resource_plot,
    line_ts_ms,
    log_has_new_columns,
    parse_tsv_row,
    read_resource_window,
    seek_to_window_start,
)

HEADER = "\t".join(LOG_COLUMNS) + "\n"
OLD_HEADER = "\t".join(REQUIRED_COLUMNS) + "\n"
TOTALS = ResourceTotals(ram_gb=134.0, cores=64.0)

# One full new-format row. Tests override only the cells they are about
# and leave the rest at these values.
ROW = {
    "elapsed_s": 2.0,
    "timestamp_ms": 1000,
    "rss": 5,
    "cpu_percent": 1.0,
    "read_bytes_per_s": 1048576,
    "write_bytes_per_s": 524288,
    "io_stall_percent": 12.5,
    "rebuild_id": 3,
}

# The same row as a parsed sample, with the new columns unset, as an
# old-format row or a server without I/O accounting writes it.
BASE_SAMPLE = ResourceSample(elapsed_s=2.0, ts_ms=1000, rss=5, cpu_percent=1.0)


def row_line(columns=LOG_COLUMNS, **overrides):
    """Render one TSV row, with the named cells replaced."""
    cells = ROW | overrides
    return "\t".join(str(cells[name]) for name in columns) + "\n"


def log_text(rows, columns=LOG_COLUMNS):
    """Render a log: a header, then one row per dict of cell overrides."""
    header = "\t".join(columns) + "\n"
    return header + "".join(row_line(columns, **row) for row in rows)


def write_log(tmp_path, rows, columns=LOG_COLUMNS):
    """Write a resource-usage TSV to a temp file and return its path."""
    path = tmp_path / "res.tsv"
    path.write_text(log_text(rows, columns))
    return path


def sample(**overrides):
    """Build a ResourceSample, with the named fields replaced."""
    return replace(BASE_SAMPLE, **overrides)


def first_in_window(text, target):
    """Emulate the reader: seek, back up, return the first ts >= target."""
    stream = io.BytesIO(text.encode())
    stream.seek(0, 2)
    size = stream.tell()
    offset = seek_to_window_start(stream, target, size)
    read_from = max(0, offset - SEEK_BACKUP_BYTES)
    stream.seek(read_from)
    if read_from > 0:
        stream.readline()
    for raw in stream:
        ts = line_ts_ms(raw)
        if ts is not None and ts >= target:
            return ts
    return None


def test_parse_row_with_every_column():
    assert parse_tsv_row(row_line()) == sample(
        read_bytes_per_s=1048576.0,
        write_bytes_per_s=524288.0,
        io_stall_percent=12.5,
        rebuild_id=3,
    )


def test_parse_old_format_row_leaves_new_columns_none():
    assert parse_tsv_row(row_line(REQUIRED_COLUMNS)) == sample()


@pytest.mark.parametrize("column", OPTIONAL_COLUMNS)
def test_parse_empty_optional_cell_is_none(column):
    parsed = parse_tsv_row(row_line(**{column: ""}))
    assert getattr(parsed, column) is None
    # The other three still parsed, so one hole does not spread.
    new_values = (
        parsed.read_bytes_per_s,
        parsed.write_bytes_per_s,
        parsed.io_stall_percent,
        parsed.rebuild_id,
    )
    assert new_values.count(None) == 1


@pytest.mark.parametrize("column", REQUIRED_COLUMNS)
def test_parse_empty_required_cell_rejects_row(column):
    assert parse_tsv_row(row_line(**{column: ""})) is None


@pytest.mark.parametrize("column", OPTIONAL_COLUMNS)
def test_parse_non_numeric_optional_cell_rejects_row(column):
    # Empty means the OS had nothing to report; garbage means a broken
    # line, and those are still dropped.
    assert parse_tsv_row(row_line(**{column: "nonsense"})) is None


@pytest.mark.parametrize(
    "line",
    [
        # Neither the old nor the new column count.
        "2.0\t1000\t5\t1.0\t9\n",
        "2.0\t1000\n",
        # Both headers are rejected by the numeric parse, not by width.
        HEADER,
        OLD_HEADER,
    ],
)
def test_parse_row_rejects_other_shapes(line):
    assert parse_tsv_row(line) is None


def test_log_has_new_columns_new_header(tmp_path):
    path = tmp_path / "res.tsv"
    path.write_text(HEADER)
    assert log_has_new_columns(path) is True


def test_log_has_new_columns_old_header(tmp_path):
    path = tmp_path / "res.tsv"
    path.write_text(OLD_HEADER)
    assert log_has_new_columns(path) is False


def test_log_has_new_columns_mixed_file_reads_as_old(tmp_path):
    # The server rotates on a format change, but if that rename failed it
    # appends a second header instead. Line 1 decides, so this reads old.
    path = tmp_path / "res.tsv"
    path.write_text(OLD_HEADER + HEADER + row_line())
    assert log_has_new_columns(path) is False


def test_log_has_new_columns_first_line_is_not_a_header(tmp_path):
    path = tmp_path / "res.tsv"
    path.write_text("garbage\n" + HEADER)
    assert log_has_new_columns(path) is False


def test_log_has_new_columns_empty_file(tmp_path):
    path = tmp_path / "res.tsv"
    path.write_text("")
    assert log_has_new_columns(path) is False


def test_log_has_new_columns_missing_file(tmp_path):
    assert log_has_new_columns(tmp_path / "does-not-exist.tsv") is False


def test_line_ts_ms_reads_the_timestamp_column():
    assert line_ts_ms(b"2.0\t1700000000000\t5000000\t50.0\n") == 1700000000000


def test_line_ts_ms_header_is_none():
    assert line_ts_ms(HEADER.encode()) is None


def test_line_ts_ms_short_line_is_none():
    assert line_ts_ms(b"2.0\n") is None


def test_line_ts_ms_non_integer_ts_is_none():
    assert line_ts_ms(b"2.0\tnot-a-number\t5\t1.0\n") is None


def test_seek_target_before_first_row_returns_zero():
    text = log_text([{"timestamp_ms": 1000}, {"timestamp_ms": 2000}])
    stream = io.BytesIO(text.encode())
    stream.seek(0, 2)
    assert seek_to_window_start(stream, 500, stream.tell()) == 0


def test_seek_finds_first_row_at_or_after_target():
    text = log_text([{"timestamp_ms": ts} for ts in range(1000, 6000, 1000)])
    assert first_in_window(text, 3000) == 3000
    assert first_in_window(text, 3500) == 4000
    assert first_in_window(text, 999) == 1000


def test_seek_target_past_last_row_finds_nothing():
    text = log_text([{"timestamp_ms": ts} for ts in range(1000, 4000, 1000)])
    assert first_in_window(text, 9999) is None


def test_seek_never_skips_the_boundary_in_a_large_file():
    # Larger than SEEK_BACKUP_BYTES so the bisect actually has to land
    # near the boundary rather than the backup covering the whole file.
    text = log_text(
        [
            {"timestamp_ms": 100000 + row_index * 1000}
            for row_index in range(4000)
        ]
    )
    for target in (100000, 1_500_000, 3_000_000, 4_099_000):
        assert first_in_window(text, target) == target


def test_read_window_returns_rows_in_range(tmp_path):
    rows = [{"timestamp_ms": ts} for ts in range(1000, 3100, 100)]
    path = write_log(tmp_path, rows)
    plot = read_resource_window(path, TOTALS, 1500, 2500, 500)
    assert plot.times_s[0] == pytest.approx(1.5)
    assert plot.times_s[-1] == pytest.approx(2.5)
    assert all(1.5 <= time_s <= 2.5 for time_s in plot.times_s)


def test_read_window_carries_totals_and_edges(tmp_path):
    path = write_log(tmp_path, [{}])
    plot = read_resource_window(path, TOTALS, 500, 1500, 500)
    assert plot.rss_total == 134.0
    assert plot.cpu_total == 64.0
    assert plot.start_s == pytest.approx(0.5)
    assert plot.end_s == pytest.approx(1.5)


def test_read_window_buckets_keep_peaks(tmp_path):
    rows = [
        {"timestamp_ms": 1050, "rss": 3_000_000_000},
        {"timestamp_ms": 1100, "rss": 5_000_000_000},
        {"timestamp_ms": 1300, "rss": 4_000_000_000},
        {"timestamp_ms": 1950, "rss": 9_000_000_000},
    ]
    path = write_log(tmp_path, rows)
    plot = read_resource_window(path, TOTALS, 1000, 2000, 5)
    assert plot.times_s == pytest.approx((1.05, 1.3, 1.95))
    assert plot.rss_gb == pytest.approx((5.0, 4.0, 9.0))


def test_read_window_never_exceeds_max_points(tmp_path):
    rows = [{"timestamp_ms": 1000 + row_index} for row_index in range(1000)]
    path = write_log(tmp_path, rows)
    plot = read_resource_window(path, TOTALS, 1000, 2000, 50)
    assert len(plot.times_s) <= 50


# A stop at ts 2000, then a restart at ts 3000 (elapsed drops 4 -> 2).
RESTART_ROWS = [
    {"elapsed_s": 2.0, "timestamp_ms": 1000},
    {"elapsed_s": 4.0, "timestamp_ms": 2000},
    {"elapsed_s": 2.0, "timestamp_ms": 3000},
    {"elapsed_s": 4.0, "timestamp_ms": 4000},
]


def test_read_window_detects_restart_with_both_edges(tmp_path):
    # Stop and start both in the window: both lines show.
    path = write_log(tmp_path, RESTART_ROWS)
    plot = read_resource_window(path, TOTALS, 0, 5000, 500)
    assert plot.stop_times_s == pytest.approx((2.0,))
    assert plot.start_times_s == pytest.approx((3.0,))


def test_read_window_start_across_window_start(tmp_path):
    # Stop is before the window, start inside it: only the start shows.
    path = write_log(tmp_path, RESTART_ROWS)
    plot = read_resource_window(path, TOTALS, 2500, 5000, 500)
    assert plot.stop_times_s == ()
    assert plot.start_times_s == pytest.approx((3.0,))


def test_read_window_stop_across_window_end(tmp_path):
    # Stop inside the window, start just past its end: the peek past the
    # window still records the stop; the start is off-screen.
    path = write_log(tmp_path, RESTART_ROWS)
    plot = read_resource_window(path, TOTALS, 0, 2500, 500)
    assert plot.stop_times_s == pytest.approx((2.0,))
    assert plot.start_times_s == ()


def test_read_window_empty_log_yields_empty_plot(tmp_path):
    path = tmp_path / "empty.tsv"
    path.write_text(HEADER)
    plot = read_resource_window(path, TOTALS, 0, 5000, 500)
    assert plot.times_s == ()
    assert plot.stop_times_s == ()
    assert plot.start_times_s == ()


def test_read_window_missing_file_yields_empty_framed_plot(tmp_path):
    plot = read_resource_window(
        tmp_path / "does-not-exist.tsv", TOTALS, 0, 5000, 500
    )
    assert plot.times_s == ()
    assert plot.start_s == pytest.approx(0.0)
    assert plot.end_s == pytest.approx(5.0)


def test_get_resource_plot_detects_a_restart():
    samples = [
        sample(elapsed_s=2.0, ts_ms=1000),
        sample(elapsed_s=4.0, ts_ms=2000),
        sample(elapsed_s=2.0, ts_ms=3000),
    ]
    plot = get_resource_plot(samples, TOTALS, 0, 5000)
    assert plot.stop_times_s == pytest.approx((2.0,))
    assert plot.start_times_s == pytest.approx((3.0,))


def test_get_resource_plot_monotonic_has_no_restart():
    samples = [
        sample(elapsed_s=elapsed, ts_ms=int(elapsed * 1000))
        for elapsed in (2.0, 4.0, 6.0, 8.0)
    ]
    plot = get_resource_plot(samples, TOTALS, 0, 100000)
    assert plot.stop_times_s == ()
    assert plot.start_times_s == ()


def test_get_resource_plot_keeps_only_windowed_samples():
    samples = [
        sample(elapsed_s=elapsed, ts_ms=int(elapsed * 1000))
        for elapsed in (1.0, 2.0, 3.0, 4.0)
    ]
    plot = get_resource_plot(samples, TOTALS, 2000, 3000)
    assert plot.times_s == pytest.approx((2.0, 3.0))


# Rebuild 3 runs at ts 2000 and 3000, with no rebuild either side of it.
REBUILD_SAMPLES = [
    sample(ts_ms=1000),
    sample(ts_ms=2000, rebuild_id=3),
    sample(ts_ms=3000, rebuild_id=3),
    sample(ts_ms=4000),
]


def test_get_resource_plot_rebuild_brackets_the_samples_that_had_it():
    # The start is the first sample carrying the id, the end the last.
    plot = get_resource_plot(REBUILD_SAMPLES, TOTALS, 0, 5000)
    assert plot.rebuild_start_times_s == pytest.approx((2.0,))
    assert plot.rebuild_end_times_s == pytest.approx((3.0,))


def test_get_resource_plot_rebuild_already_running_has_no_start():
    # The window opens mid-rebuild, so its start is not in the data. The
    # end still shows.
    samples = [
        sample(ts_ms=1000, rebuild_id=3),
        sample(ts_ms=2000, rebuild_id=3),
        sample(ts_ms=3000),
    ]
    plot = get_resource_plot(samples, TOTALS, 0, 5000)
    assert plot.rebuild_start_times_s == ()
    assert plot.rebuild_end_times_s == pytest.approx((2.0,))


def test_get_resource_plot_rebuild_spanning_every_sample_has_no_markers():
    samples = [sample(ts_ms=ts, rebuild_id=3) for ts in (1000, 2000, 3000)]
    plot = get_resource_plot(samples, TOTALS, 0, 5000)
    assert plot.rebuild_start_times_s == ()
    assert plot.rebuild_end_times_s == ()


def test_get_resource_plot_new_rebuild_id_ends_the_previous_one():
    # Needs two rebuilds inside one sampling interval, so in practice
    # this is a server restart renumbering ids while a rebuild ran.
    samples = [
        sample(ts_ms=1000),
        sample(ts_ms=2000, rebuild_id=3),
        sample(ts_ms=3000, rebuild_id=4),
        sample(ts_ms=4000),
    ]
    plot = get_resource_plot(samples, TOTALS, 0, 5000)
    assert plot.rebuild_start_times_s == pytest.approx((2.0, 3.0))
    assert plot.rebuild_end_times_s == pytest.approx((2.0, 3.0))


def test_get_resource_plot_rebuild_start_before_window():
    plot = get_resource_plot(REBUILD_SAMPLES, TOTALS, 2500, 5000)
    assert plot.rebuild_start_times_s == ()
    assert plot.rebuild_end_times_s == pytest.approx((3.0,))


def test_get_resource_plot_rebuild_end_after_window():
    plot = get_resource_plot(REBUILD_SAMPLES, TOTALS, 0, 2500)
    assert plot.rebuild_start_times_s == pytest.approx((2.0,))
    assert plot.rebuild_end_times_s == ()


def test_get_resource_plot_old_format_samples_have_no_rebuilds():
    samples = [sample(ts_ms=ts) for ts in (1000, 2000, 3000)]
    plot = get_resource_plot(samples, TOTALS, 0, 5000)
    assert plot.rebuild_start_times_s == ()
    assert plot.rebuild_end_times_s == ()


# The same rebuild as REBUILD_SAMPLES, as log rows. An empty cell is how
# the server writes "no rebuild running".
REBUILD_ROWS = [
    {"timestamp_ms": 1000, "rebuild_id": ""},
    {"timestamp_ms": 2000, "rebuild_id": 3},
    {"timestamp_ms": 3000, "rebuild_id": 3},
    {"timestamp_ms": 4000, "rebuild_id": ""},
]


def test_read_window_detects_rebuild_with_both_edges(tmp_path):
    path = write_log(tmp_path, REBUILD_ROWS)
    plot = read_resource_window(path, TOTALS, 0, 5000, 500)
    assert plot.rebuild_start_times_s == pytest.approx((2.0,))
    assert plot.rebuild_end_times_s == pytest.approx((3.0,))


def test_read_window_rebuild_end_across_window_end(tmp_path):
    # The rebuild is still running when the window closes at 3500. The
    # row past the window is tracked before the loop breaks, so its end
    # lands on the last in-window sample.
    path = write_log(tmp_path, REBUILD_ROWS)
    plot = read_resource_window(path, TOTALS, 0, 3500, 500)
    assert plot.rebuild_start_times_s == pytest.approx((2.0,))
    assert plot.rebuild_end_times_s == pytest.approx((3.0,))


def test_read_window_rebuild_start_across_window_start(tmp_path):
    # The rebuild began before the window, so only its end shows.
    path = write_log(tmp_path, REBUILD_ROWS)
    plot = read_resource_window(path, TOTALS, 2500, 5000, 500)
    assert plot.rebuild_start_times_s == ()
    assert plot.rebuild_end_times_s == pytest.approx((3.0,))


def test_read_window_old_format_log_has_no_rebuilds(tmp_path):
    # No rebuild column at all, so the markers degrade to nothing with no
    # format check anywhere in the read path.
    path = write_log(tmp_path, REBUILD_ROWS, columns=REQUIRED_COLUMNS)
    plot = read_resource_window(path, TOTALS, 0, 5000, 500)
    assert plot.rebuild_start_times_s == ()
    assert plot.rebuild_end_times_s == ()
