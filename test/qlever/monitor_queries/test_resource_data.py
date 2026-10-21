"""Tests for the resource data layer: parse, tail, seek, windowed read, plot."""

import io
from dataclasses import replace
from math import ceil, isnan

import pytest

from qlever.monitor_queries.log_reader import CompletedQuery
from qlever.monitor_queries.models import ResourceWindow
from qlever.monitor_queries.resource_data import (
    COLUMNS,
    LIVE_RESOURCE_BUFFER_MS,
    LIVE_WINDOW_PRESETS,
    MAX_BUCKETS,
    Capacity,
    Column,
    EventTracker,
    SampleBuffer,
    bucket_count,
    bucket_value,
    bucket_width_ms,
    coverage_note,
    live_buffer_span_ms,
    live_window_bounds,
    read_resource_window,
    sample_count,
    window_for_samples,
)
from qlever.monitor_queries.resource_reader import (
    LOG_COLUMNS,
    OPTIONAL_COLUMNS,
    REQUIRED_COLUMNS,
    SEEK_BACKUP_BYTES,
    Sample,
    iter_samples,
    line_ts_ms,
    log_has_new_columns,
    parse_tsv_row,
    seek_to_window_start,
)
from qlever.monitor_queries.widgets.window_stepper import preset_ms

HEADER = "\t".join(LOG_COLUMNS) + "\n"
OLD_HEADER = "\t".join(REQUIRED_COLUMNS) + "\n"
CAPACITY = Capacity(ram_gb=134.0, cores=64.0)

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
    "index_rebuild_id": 3,
}

# The same row as a parsed sample, with the new columns unset, as an
# old-format row or a server without I/O accounting writes it.
BASE_SAMPLE = Sample(elapsed_s=2.0, ts_ms=1000, rss=5, cpu_percent=1.0)


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
    """Build a Sample, with the named fields replaced."""
    return replace(BASE_SAMPLE, **overrides)


def operation(end_ms, duration_ms, op_type="query"):
    """A finished operation. Only these three fields reach the buckets."""
    return CompletedQuery(
        start_ms=end_ms - duration_ms,
        end_ms=end_ms,
        duration_ms=duration_ms,
        status="ok",
        start_line_offset=None,
        op_type=op_type,
    )


def build_window(
    samples, start_ms, end_ms, buckets, operations=(), capacity=CAPACITY
):
    """Bucket samples into one window, with no operations by default."""
    return window_for_samples(
        samples=samples,
        operations=operations,
        capacity=capacity,
        start_ms=start_ms,
        end_ms=end_ms,
        buckets=buckets,
    )


def window_from_log(path, start_ms, end_ms, buckets, operations=()):
    """Read one window off a log, with the totals these tests use."""
    return read_resource_window(
        path=path,
        operations=operations,
        capacity=CAPACITY,
        start_ms=start_ms,
        end_ms=end_ms,
        buckets=buckets,
    )


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
        index_rebuild_id=3,
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
        parsed.index_rebuild_id,
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


def sample_times_ms(text, start_ms, end_ms, should_cancel=None):
    """Timestamps `iter_samples` yields for a window of this log."""
    stream = io.BytesIO(text.encode())
    return [
        sample.ts_ms
        for sample in iter_samples(stream, start_ms, end_ms, should_cancel)
    ]


def test_iter_samples_covers_the_window():
    text = log_text([{"timestamp_ms": ts} for ts in range(1000, 6000, 1000)])
    assert 3000 in sample_times_ms(text, 3000, 4000)
    assert 4000 in sample_times_ms(text, 3000, 4000)


def test_iter_samples_yields_one_row_past_the_window_end():
    text = log_text([{"timestamp_ms": ts} for ts in range(1000, 6000, 1000)])
    times = sample_times_ms(text, 2000, 3000)
    # 4000 is out of the window, but an event at 3000 needs it to pair.
    assert times[-1] == 4000


def test_iter_samples_yields_rows_before_the_window_start():
    text = log_text([{"timestamp_ms": ts} for ts in range(1000, 6000, 1000)])
    # The backup before the seek boundary brings earlier rows along, and
    # the trackers need them to spot a restart that began before 3000.
    assert min(sample_times_ms(text, 3000, 4000)) < 3000


def test_iter_samples_skips_the_header():
    text = log_text([{"timestamp_ms": 1000}])
    assert sample_times_ms(text, 0, 9999) == [1000]


def test_iter_samples_stops_when_cancelled():
    rows = [{"timestamp_ms": 1000 + row * 10} for row in range(200_000)]
    text = log_text(rows)
    times = sample_times_ms(text, 0, 9_999_999, should_cancel=lambda: True)
    assert 0 < len(times) < len(rows)


def test_iter_samples_empty_log_yields_nothing():
    assert sample_times_ms(log_text([]), 0, 9999) == []


def events_of(window):
    """The window's events as (kind, time_s) pairs."""
    return [(event.kind, event.time_s) for event in window.events]


def test_read_window_returns_rows_in_range(tmp_path):
    rows = [{"timestamp_ms": ts} for ts in range(1000, 3100, 100)]
    path = write_log(tmp_path, rows)
    window = window_from_log(path, 1500, 2500, 500)
    assert window.times_s[0] == pytest.approx(1.5)
    assert window.times_s[-1] == pytest.approx(2.5)
    assert all(1.5 <= time_s <= 2.5 for time_s in window.times_s)


def test_read_window_carries_capacity_and_edges(tmp_path):
    path = write_log(tmp_path, [{}])
    window = window_from_log(path, 500, 1500, 500)
    assert window.series["rss"].total == 134.0
    assert window.series["cpu_percent"].total == 64.0
    assert window.start_s == pytest.approx(0.5)
    assert window.end_s == pytest.approx(1.5)


def test_read_window_buckets_keep_peaks(tmp_path):
    rows = [
        {"timestamp_ms": 1050, "rss": 3_000_000_000},
        {"timestamp_ms": 1100, "rss": 5_000_000_000},
        {"timestamp_ms": 1300, "rss": 4_000_000_000},
        {"timestamp_ms": 1950, "rss": 9_000_000_000},
    ]
    path = write_log(tmp_path, rows)
    window = window_from_log(path, 1000, 2000, 5)
    assert window.times_s == pytest.approx((1.05, 1.3, 1.95))
    assert window.series["rss"].values == pytest.approx((5.0, 4.0, 9.0))


def test_read_window_never_exceeds_the_bucket_count(tmp_path):
    rows = [{"timestamp_ms": 1000 + row_index} for row_index in range(1000)]
    path = write_log(tmp_path, rows)
    window = window_from_log(path, 1000, 2000, 50)
    assert len(window.times_s) <= 50


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
    window = window_from_log(path, 0, 5000, 500)
    assert events_of(window) == [("server_down", 2.0), ("server_up", 3.0)]


def test_read_window_start_across_window_start(tmp_path):
    # Stop is before the window, start inside it: only the start shows.
    path = write_log(tmp_path, RESTART_ROWS)
    window = window_from_log(path, 2500, 5000, 500)
    assert events_of(window) == [("server_up", 3.0)]


def test_read_window_stop_across_window_end(tmp_path):
    # Stop inside the window, start just past its end: the row read past
    # the window still records the stop; the start is off-screen.
    path = write_log(tmp_path, RESTART_ROWS)
    window = window_from_log(path, 0, 2500, 500)
    assert events_of(window) == [("server_down", 2.0)]


def test_read_window_empty_log_yields_an_empty_window(tmp_path):
    path = tmp_path / "empty.tsv"
    path.write_text(HEADER)
    window = window_from_log(path, 0, 5000, 500)
    assert window.times_s == ()
    assert window.events == ()


def test_read_window_missing_file_frames_the_time_range(tmp_path):
    window = window_from_log(tmp_path / "does-not-exist.tsv", 0, 5000, 500)
    assert window.times_s == ()
    assert window.start_s == pytest.approx(0.0)
    assert window.end_s == pytest.approx(5.0)


def tracked(samples, start_ms=0, end_ms=100_000):
    """Feed samples to an EventTracker and return (kind, time_s) pairs."""
    tracker = EventTracker(start_ms, end_ms)
    for one in samples:
        tracker.track(one)
    return [(event.kind, event.time_s) for event in tracker.events]


def test_event_tracker_no_events_for_a_quiet_log():
    samples = [sample(elapsed_s=index, ts_ms=index * 1000) for index in (1, 2)]
    assert tracked(samples) == []


def test_event_tracker_elapsed_drop_is_a_restart():
    samples = [
        sample(elapsed_s=2.0, ts_ms=1000),
        sample(elapsed_s=4.0, ts_ms=2000),
        sample(elapsed_s=2.0, ts_ms=3000),
    ]
    assert tracked(samples) == [("server_down", 2.0), ("server_up", 3.0)]


def test_event_tracker_rebuild_appearing_has_no_end():
    samples = [sample(ts_ms=1000), sample(ts_ms=2000, index_rebuild_id=3)]
    assert tracked(samples) == [("rebuild_start", 2.0)]


def test_event_tracker_rebuild_vanishing_has_no_start():
    samples = [sample(ts_ms=1000, index_rebuild_id=3), sample(ts_ms=2000)]
    assert tracked(samples) == [("rebuild_end", 1.0)]


def test_event_tracker_new_rebuild_id_ends_the_previous_one():
    samples = [
        sample(ts_ms=1000, index_rebuild_id=3),
        sample(ts_ms=2000, index_rebuild_id=4),
    ]
    assert tracked(samples) == [
        ("rebuild_end", 1.0),
        ("rebuild_start", 2.0),
    ]


def test_event_tracker_restart_and_rebuild_end_stay_in_time_order():
    samples = [
        sample(elapsed_s=10.0, ts_ms=3000, index_rebuild_id=3),
        sample(elapsed_s=1.0, ts_ms=4000),
    ]
    # Both events at 3.0 come before the one at 4.0, unsorted.
    assert tracked(samples) == [
        ("server_down", 3.0),
        ("rebuild_end", 3.0),
        ("server_up", 4.0),
    ]


def test_event_tracker_keeps_the_half_of_a_restart_inside_the_window():
    samples = [
        sample(elapsed_s=10.0, ts_ms=1000),
        sample(elapsed_s=1.0, ts_ms=3000),
    ]
    # The server went down before the window opened, so only the
    # coming back up is visible.
    assert tracked(samples, start_ms=2000, end_ms=5000) == [("server_up", 3.0)]


def test_event_tracker_drops_events_outside_the_window():
    samples = [
        sample(elapsed_s=10.0, ts_ms=1000),
        sample(elapsed_s=1.0, ts_ms=2000),
    ]
    assert tracked(samples, start_ms=5000, end_ms=9000) == []


def test_event_tracker_old_format_samples_have_no_rebuilds():
    samples = [sample(ts_ms=ts, index_rebuild_id=None) for ts in (1000, 2000)]
    assert tracked(samples) == []


# Rebuild 3 runs at ts 2000 and 3000, with no rebuild either side of it.
REBUILD_SAMPLES = [
    sample(ts_ms=1000),
    sample(ts_ms=2000, index_rebuild_id=3),
    sample(ts_ms=3000, index_rebuild_id=3),
    sample(ts_ms=4000),
]


def test_event_tracker_rebuild_spanning_every_sample_has_no_events():
    samples = [
        sample(ts_ms=ts, index_rebuild_id=3) for ts in (1000, 2000, 3000)
    ]
    assert tracked(samples) == []


def test_event_tracker_rebuild_start_before_the_window():
    # The rebuild began before the window opened, so only its end shows.
    assert tracked(REBUILD_SAMPLES, start_ms=2500, end_ms=5000) == [
        ("rebuild_end", 3.0)
    ]


def test_event_tracker_rebuild_end_after_the_window():
    assert tracked(REBUILD_SAMPLES, start_ms=0, end_ms=2500) == [
        ("rebuild_start", 2.0)
    ]


# The same rebuild as REBUILD_SAMPLES, as log rows. An empty cell is how
# the server writes "no rebuild running".
REBUILD_ROWS = [
    {"timestamp_ms": 1000, "index_rebuild_id": ""},
    {"timestamp_ms": 2000, "index_rebuild_id": 3},
    {"timestamp_ms": 3000, "index_rebuild_id": 3},
    {"timestamp_ms": 4000, "index_rebuild_id": ""},
]


def test_read_window_detects_rebuild_with_both_edges(tmp_path):
    path = write_log(tmp_path, REBUILD_ROWS)
    window = window_from_log(path, 0, 5000, 500)
    assert events_of(window) == [
        ("rebuild_start", 2.0),
        ("rebuild_end", 3.0),
    ]


def test_read_window_rebuild_end_across_window_end(tmp_path):
    # The rebuild is still running when the window closes at 3500. The
    # row read past the window is tracked, so its end lands on the last
    # in-window sample.
    path = write_log(tmp_path, REBUILD_ROWS)
    window = window_from_log(path, 0, 3500, 500)
    assert events_of(window) == [
        ("rebuild_start", 2.0),
        ("rebuild_end", 3.0),
    ]


def test_read_window_rebuild_start_across_window_start(tmp_path):
    # The rebuild began before the window, so only its end shows.
    path = write_log(tmp_path, REBUILD_ROWS)
    window = window_from_log(path, 2500, 5000, 500)
    assert events_of(window) == [("rebuild_end", 3.0)]


def test_read_window_old_format_log_has_no_rebuilds(tmp_path):
    # No rebuild column at all, so the markers degrade to nothing with no
    # format check anywhere in the read path.
    path = write_log(tmp_path, REBUILD_ROWS, columns=REQUIRED_COLUMNS)
    window = window_from_log(path, 0, 5000, 500)
    assert events_of(window) == []


def test_window_peak_column_keeps_the_bucket_maximum():
    samples = [
        sample(ts_ms=1100, rss=3_000_000_000),
        sample(ts_ms=1600, rss=5_000_000_000),
    ]
    window = build_window(samples, 1000, 5000, 4)
    assert window.series["rss"].values == pytest.approx((5.0,))


def test_window_mean_column_averages_the_bucket():
    samples = [
        sample(ts_ms=1100, read_bytes_per_s=1e6),
        sample(ts_ms=1600, read_bytes_per_s=3e6),
    ]
    window = build_window(samples, 1000, 5000, 4)
    assert window.series["read_bytes_per_s"].values == pytest.approx((2.0,))


def test_window_skips_buckets_with_no_sample():
    samples = [sample(ts_ms=1100), sample(ts_ms=3200)]
    window = build_window(samples, 1000, 5000, 4)
    assert window.times_s == pytest.approx((1.1, 3.2))


def test_window_every_series_is_as_long_as_times_s():
    samples = [sample(ts_ms=ts, read_bytes_per_s=1e6) for ts in (1100, 3200)]
    window = build_window(samples, 1000, 5000, 4)
    for series in window.series.values():
        assert len(series.values) == len(window.times_s)


def test_window_omits_a_column_that_never_reported():
    window = build_window([sample(ts_ms=1100)], 1000, 5000, 4)
    assert "read_bytes_per_s" not in window.series
    assert "io_stall_percent" not in window.series


def test_window_keeps_the_required_columns_with_no_samples():
    window = build_window([], 1000, 5000, 4)
    assert window.series["rss"].values == ()
    assert window.series["cpu_percent"].label == "CPU"


def test_window_gap_where_a_column_reported_nothing():
    # The server began reporting disk I/O part way through the window.
    samples = [
        sample(ts_ms=1100),
        sample(ts_ms=3200, read_bytes_per_s=2e6),
    ]
    window = build_window(samples, 1000, 5000, 4)
    quiet, reported = window.series["read_bytes_per_s"].values
    assert isnan(quiet)
    assert reported == pytest.approx(2.0)


def test_window_rate_columns_have_no_capacity():
    samples = [sample(ts_ms=1100, read_bytes_per_s=1e6)]
    window = build_window(samples, 1000, 5000, 4)
    assert window.series["read_bytes_per_s"].total is None


def test_window_unknown_core_count_leaves_the_cpu_capacity_none():
    capacity = Capacity(ram_gb=134.0, cores=None)
    samples = [sample(ts_ms=1100)]
    window = build_window(samples, 1000, 5000, 4, capacity=capacity)
    assert window.series["cpu_percent"].total is None
    assert window.series["rss"].total == 134.0


def test_window_frames_the_edges_with_no_samples():
    window = build_window([], 1000, 5000, 4)
    assert window.start_s == pytest.approx(1.0)
    assert window.end_s == pytest.approx(5.0)
    assert window.times_s == ()


def test_window_never_exceeds_the_bucket_count():
    samples = [sample(ts_ms=1000 + index) for index in range(1000)]
    window = build_window(samples, 1000, 2000, 50)
    assert len(window.times_s) <= 50


# Samples at 1100 and 3200 fill buckets 0 and 2 of four, so the grid is
# (1.1, 3.2) and bucket 1 is off it. The operation tests all read
# against this.
SAMPLED = [sample(ts_ms=1100), sample(ts_ms=3200)]


def test_window_operation_means_are_kept_per_type():
    operations = [
        operation(1500, 800),
        operation(1800, 400),
        operation(1600, 200, "update"),
        operation(3500, 1000),
    ]
    window = build_window(SAMPLED, 1000, 5000, 4, operations=operations)
    query_values = window.series["query_mean_ms"].values
    update_values = window.series["update_mean_ms"].values
    assert query_values == pytest.approx((600.0, 1000.0))
    assert update_values[0] == pytest.approx(200.0)


def test_window_bucket_with_none_of_a_type_is_a_gap():
    operations = [operation(1600, 200, "update")]
    window = build_window(SAMPLED, 1000, 5000, 4, operations=operations)
    reported, quiet = window.series["update_mean_ms"].values
    assert reported == pytest.approx(200.0)
    # No update finished in that bucket, which is not the same as one
    # taking no time.
    assert isnan(quiet)


@pytest.mark.parametrize("op_type", [None, "graph-store"])
def test_window_operation_of_no_known_type_is_left_out(op_type):
    operations = [operation(1500, 800, op_type)]
    window = build_window(SAMPLED, 1000, 5000, 4, operations=operations)
    assert "query_mean_ms" not in window.series
    assert "update_mean_ms" not in window.series


@pytest.mark.parametrize("end_ms", [900, 5100])
def test_window_operation_outside_the_window_is_left_out(end_ms):
    operations = [operation(end_ms, 800)]
    window = build_window(SAMPLED, 1000, 5000, 4, operations=operations)
    assert "query_mean_ms" not in window.series


@pytest.mark.parametrize(
    ("end_ms", "kept_at"),
    [
        # 700 ms from the sample at 3200, 1400 ms from the one at 1100.
        (2500, 1),
        # 900 ms from 1100, 1200 ms from 3200.
        (2000, 0),
        # 1050 ms from both, so the earlier one wins.
        (2150, 0),
    ],
)
def test_window_operation_with_no_sample_moves_to_the_nearest(end_ms, kept_at):
    # Bucket 1 has no sample, so the operation moves to bucket 0 or 2.
    operations = [operation(end_ms, 800)]
    window = build_window(SAMPLED, 1000, 5000, 4, operations=operations)
    values = window.series["query_mean_ms"].values
    assert values[kept_at] == pytest.approx(800.0)
    assert isnan(values[1 - kept_at])


@pytest.mark.parametrize(("end_ms", "kept"), [(5900, True), (6100, False)])
def test_window_operation_moves_at_most_the_limit(end_ms, kept):
    # A 19 s hole between samples, in one-second buckets. 5900 is 4.9 s
    # from the sample at 1000, and 6100 is 5.1 s from it.
    samples = [sample(ts_ms=1000), sample(ts_ms=20000)]
    operations = [operation(end_ms, 800)]
    window = build_window(samples, 0, 21000, 21, operations=operations)
    assert ("query_mean_ms" in window.series) == kept


def test_window_operation_with_no_sample_anywhere_is_left_out():
    operations = [operation(1500, 800)]
    window = build_window([], 1000, 5000, 4, operations=operations)
    assert "query_mean_ms" not in window.series


def test_window_with_more_buckets_than_samples_keeps_every_operation():
    # A wide plot: a sample each second, cut into 150 ms buckets, so
    # most buckets have none. Each operation sits between two samples.
    samples = [sample(ts_ms=second * 1000) for second in range(60)]
    operations = [operation(second * 1000 + 500, 100) for second in range(60)]
    window = build_window(samples, 0, 60000, 400, operations=operations)
    values = window.series["query_mean_ms"].values
    assert len([value for value in values if not isnan(value)]) == 60


def test_window_without_operations_omits_both_series():
    window = build_window(SAMPLED, 1000, 5000, 4)
    assert "query_mean_ms" not in window.series
    assert "update_mean_ms" not in window.series


def test_window_operation_series_lines_up_with_times_s():
    operations = [operation(1500, 800)]
    window = build_window(SAMPLED, 1000, 5000, 4, operations=operations)
    values = window.series["query_mean_ms"].values
    assert len(values) == len(window.times_s)


def test_window_operation_series_has_no_capacity():
    operations = [operation(1500, 800)]
    window = build_window(SAMPLED, 1000, 5000, 4, operations=operations)
    series = window.series["query_mean_ms"]
    assert series.total is None
    assert series.unit == "ms"


def test_bucket_value_of_an_empty_bucket_is_a_gap():
    assert isnan(bucket_value(COLUMNS[0], 0.0, 0))


def test_bucket_value_rejects_an_unknown_reducer():
    column = Column(
        key="rss",
        label="RSS",
        unit="GB",
        scale=1.0,
        reduce="median",
        capacity=None,
    )
    with pytest.raises(ValueError):
        bucket_value(column, 5.0, 1)


def test_sample_count_follows_the_interval():
    assert sample_count(300_000, 1) == 300
    assert sample_count(300_000, 5) == 60


def test_sample_count_of_a_span_below_one_interval_is_one():
    assert sample_count(2_000, 5) == 1


def window_reaching_back(gap_s: float, span_s: float = 3600.0):
    """A window of `span_s` whose readings start `gap_s` after its start."""
    return ResourceWindow(
        start_s=0.0,
        end_s=span_s,
        times_s=(gap_s,),
        series={},
        events=(),
    )


def test_coverage_note_is_empty_when_readings_fill_the_window():
    assert coverage_note(window_reaching_back(0.0), 1) == ""


def test_coverage_note_is_empty_within_one_sampling_interval():
    # No reading could land in a gap this short, so it is not a gap.
    assert coverage_note(window_reaching_back(0.9), 1) == ""
    assert coverage_note(window_reaching_back(4.0), 5) == ""


def test_coverage_note_counts_minutes_from_the_oldest_reading():
    assert coverage_note(window_reaching_back(3000.0), 1) == "10m of data"


def test_coverage_note_counts_seconds_below_a_minute():
    assert coverage_note(window_reaching_back(3555.0), 1) == "45s of data"


def test_coverage_note_rounds_minutes_down():
    # 10m 59s of readings never claims 11 minutes.
    assert coverage_note(window_reaching_back(2941.0), 1) == "10m of data"


def test_coverage_note_of_an_empty_window_is_empty():
    assert (
        coverage_note(
            ResourceWindow(
                start_s=0.0, end_s=300.0, times_s=(), series={}, events=()
            ),
            1,
        )
        == ""
    )


def test_live_window_presets_stay_within_the_buffer():
    widths = [preset_ms(preset) for preset in LIVE_WINDOW_PRESETS]
    assert max(widths) == LIVE_RESOURCE_BUFFER_MS
    assert widths == sorted(widths)


# A moment on a whole minute, where the Live window tests start.
ON_THE_MINUTE_MS = 1_760_000_400_000

INTERVALS_S = [1, 2, 5, 7]
SPANS_MS = [
    5 * 60_000,
    30 * 60_000,
    3_600_000,
    86_400_000,
    30 * 86_400_000 + 1_234_567,
]
HOUR_MS = 3_600_000


def bounds_width_ms(bounds):
    """Width of one bucket in a (start_ms, end_ms, buckets) answer."""
    start_ms, end_ms, buckets = bounds
    return (end_ms - start_ms) // buckets


@pytest.mark.parametrize("interval_s", INTERVALS_S)
@pytest.mark.parametrize("span_ms", SPANS_MS)
def test_bucket_width_is_whole_intervals(interval_s, span_ms):
    assert bucket_width_ms(span_ms, interval_s) % (interval_s * 1000) == 0


@pytest.mark.parametrize("interval_s", INTERVALS_S)
@pytest.mark.parametrize("span_ms", SPANS_MS)
def test_bucket_count_stays_within_the_cap(interval_s, span_ms):
    assert bucket_count(span_ms, interval_s) <= MAX_BUCKETS


@pytest.mark.parametrize("interval_s", INTERVALS_S)
@pytest.mark.parametrize("span_ms", SPANS_MS)
def test_bucket_width_is_the_narrowest_that_fits(interval_s, span_ms):
    # One interval narrower would need more buckets than the cap.
    interval_ms = interval_s * 1000
    width_ms = bucket_width_ms(span_ms, interval_s)
    if width_ms > interval_ms:
        assert ceil(span_ms / (width_ms - interval_ms)) > MAX_BUCKETS


@pytest.mark.parametrize("interval_s", INTERVALS_S)
def test_bucket_count_keeps_every_reading_up_to_the_cap(interval_s):
    interval_ms = interval_s * 1000
    full_ms = MAX_BUCKETS * interval_ms
    assert bucket_width_ms(full_ms, interval_s) == interval_ms
    assert bucket_count(full_ms, interval_s) == MAX_BUCKETS
    # One reading more and two share a bucket.
    assert bucket_width_ms(full_ms + interval_ms, interval_s) > interval_ms


@pytest.mark.parametrize("interval_s", INTERVALS_S)
@pytest.mark.parametrize("span_ms", SPANS_MS)
def test_live_window_bounds_use_the_shared_width(interval_s, span_ms):
    bounds = live_window_bounds(ON_THE_MINUTE_MS, span_ms, interval_s)
    assert bounds_width_ms(bounds) == bucket_width_ms(span_ms, interval_s)
    assert bounds[2] == bucket_count(span_ms, interval_s)


def hour_edge_and_width_ms():
    """A bucket edge of the hour window at 2 s, and its bucket width."""
    width_ms = bucket_width_ms(HOUR_MS, 2)
    return ON_THE_MINUTE_MS // width_ms * width_ms, width_ms


def test_live_window_bounds_end_on_the_edge_after_now():
    edge_ms, width_ms = hour_edge_and_width_ms()
    start_ms, end_ms, buckets = live_window_bounds(
        edge_ms + width_ms // 2, HOUR_MS, 2
    )
    assert end_ms == edge_ms + width_ms
    assert end_ms - start_ms == buckets * width_ms


def test_live_window_bounds_hold_still_within_a_bucket():
    edge_ms, width_ms = hour_edge_and_width_ms()
    early = live_window_bounds(edge_ms + 1, HOUR_MS, 2)
    late = live_window_bounds(edge_ms + width_ms - 1, HOUR_MS, 2)
    assert early == late


def test_live_window_bounds_move_one_bucket_at_an_edge():
    edge_ms, width_ms = hour_edge_and_width_ms()
    start_ms, end_ms, buckets = live_window_bounds(
        edge_ms + width_ms - 1, HOUR_MS, 2
    )
    assert live_window_bounds(edge_ms + width_ms, HOUR_MS, 2) == (
        start_ms + width_ms,
        end_ms + width_ms,
        buckets,
    )


def test_live_window_bounds_cover_a_span_the_width_does_not_divide():
    # Five minutes in 7 s buckets needs 42.9 of them, so 43 whole ones
    # reach one second past the five minutes.
    bounds = live_window_bounds(ON_THE_MINUTE_MS, 300_000, 7)
    start_ms, end_ms, buckets = bounds
    assert bounds_width_ms(bounds) == 7_000
    assert buckets == 43
    assert end_ms - start_ms == 301_000
    assert start_ms % 7_000 == 0


@pytest.mark.parametrize("interval_s", INTERVALS_S)
def test_live_buffer_span_is_whole_buckets_just_covering_the_hour(
    interval_s,
):
    width_ms = bucket_width_ms(LIVE_RESOURCE_BUFFER_MS, interval_s)
    span_ms = live_buffer_span_ms(interval_s)
    assert span_ms % width_ms == 0
    assert (
        LIVE_RESOURCE_BUFFER_MS <= span_ms < LIVE_RESOURCE_BUFFER_MS + width_ms
    )


@pytest.mark.parametrize("interval_s", INTERVALS_S)
def test_sample_buffer_holds_one_sample_more_than_its_span(interval_s):
    span_ms = live_buffer_span_ms(interval_s)
    assert SampleBuffer(interval_s).size == span_ms // (interval_s * 1000) + 1


@pytest.mark.parametrize("interval_s", [1, 2, 5, 7, 11])
def test_a_full_sample_buffer_reaches_the_widest_window_start(interval_s):
    size = SampleBuffer(interval_s).size
    interval_ms = interval_s * 1000
    # The newest sample taken right now is the worst case, since an older
    # one only pushes the oldest sample further back.
    for now_ms in range(ON_THE_MINUTE_MS, ON_THE_MINUTE_MS + 30_000, 250):
        oldest_ms = now_ms - (size - 1) * interval_ms
        start_ms, _, _ = live_window_bounds(
            now_ms, LIVE_RESOURCE_BUFFER_MS, interval_s
        )
        assert oldest_ms <= start_ms
