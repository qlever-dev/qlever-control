"""Tests for the resource data layer: parse, tail, seek, windowed read, plot."""

import io

import pytest

from qlever.monitor_queries.models import ResourceSample, ResourceTotals
from qlever.monitor_queries.resource_data import (
    SEEK_BACKUP_BYTES,
    get_resource_plot,
    line_ts_ms,
    read_resource_window,
    seek_to_window_start,
)

HEADER = "elapsed_s\ttimestamp_ms\trss\tcpu_percent\n"
TOTALS = ResourceTotals(ram_gb=134.0, cores=64.0)


def format_rows(rows):
    """Render (elapsed, ts, rss, cpu) tuples as TSV lines with a header."""
    lines = [HEADER]
    for elapsed, ts, rss, cpu in rows:
        lines.append(f"{elapsed}\t{ts}\t{rss}\t{cpu}\n")
    return "".join(lines)


def write_log(tmp_path, rows):
    """Write a resource-usage TSV to a temp file and return its path."""
    path = tmp_path / "res.tsv"
    path.write_text(format_rows(rows))
    return path


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


def sample(elapsed, ts, rss, cpu):
    """Build a ResourceSample from raw source units."""
    return ResourceSample(
        elapsed_s=elapsed, ts_ms=ts, rss=rss, cpu_percent=cpu
    )


def test_line_ts_ms_reads_the_timestamp_column():
    assert line_ts_ms(b"2.0\t1700000000000\t5000000\t50.0\n") == 1700000000000


def test_line_ts_ms_header_is_none():
    assert line_ts_ms(HEADER.encode()) is None


def test_line_ts_ms_short_line_is_none():
    assert line_ts_ms(b"2.0\n") is None


def test_line_ts_ms_non_integer_ts_is_none():
    assert line_ts_ms(b"2.0\tnot-a-number\t5\t1.0\n") is None


def test_seek_target_before_first_row_returns_zero():
    text = format_rows([(2.0, 1000, 5, 1.0), (4.0, 2000, 6, 1.0)])
    stream = io.BytesIO(text.encode())
    stream.seek(0, 2)
    assert seek_to_window_start(stream, 500, stream.tell()) == 0


def test_seek_finds_first_row_at_or_after_target():
    rows = [(2.0, ts, 5, 1.0) for ts in range(1000, 6000, 1000)]
    text = format_rows(rows)
    assert first_in_window(text, 3000) == 3000
    assert first_in_window(text, 3500) == 4000
    assert first_in_window(text, 999) == 1000


def test_seek_target_past_last_row_finds_nothing():
    rows = [(2.0, ts, 5, 1.0) for ts in range(1000, 4000, 1000)]
    assert first_in_window(format_rows(rows), 9999) is None


def test_seek_never_skips_the_boundary_in_a_large_file():
    # Larger than SEEK_BACKUP_BYTES so the bisect actually has to land
    # near the boundary rather than the backup covering the whole file.
    rows = [
        (2.0, 100000 + row_index * 1000, 5, 1.0) for row_index in range(4000)
    ]
    text = format_rows(rows)
    for target in (100000, 1_500_000, 3_000_000, 4_099_000):
        assert first_in_window(text, target) == target


def test_read_window_returns_rows_in_range(tmp_path):
    rows = [(2.0, ts, 1_000_000_000, 50.0) for ts in range(1000, 3100, 100)]
    path = write_log(tmp_path, rows)
    plot = read_resource_window(path, TOTALS, 1500, 2500, 500)
    assert plot.times_s[0] == pytest.approx(1.5)
    assert plot.times_s[-1] == pytest.approx(2.5)
    assert all(1.5 <= time_s <= 2.5 for time_s in plot.times_s)


def test_read_window_carries_totals_and_edges(tmp_path):
    path = write_log(tmp_path, [(2.0, 1000, 5, 1.0)])
    plot = read_resource_window(path, TOTALS, 500, 1500, 500)
    assert plot.rss_total == 134.0
    assert plot.cpu_total == 64.0
    assert plot.start_s == pytest.approx(0.5)
    assert plot.end_s == pytest.approx(1.5)


def test_read_window_buckets_keep_peaks(tmp_path):
    rows = [
        (2.0, 1050, 3_000_000_000, 50.0),
        (4.0, 1100, 5_000_000_000, 50.0),
        (6.0, 1300, 4_000_000_000, 50.0),
        (8.0, 1950, 9_000_000_000, 50.0),
    ]
    path = write_log(tmp_path, rows)
    plot = read_resource_window(path, TOTALS, 1000, 2000, 5)
    assert plot.times_s == pytest.approx((1.05, 1.3, 1.95))
    assert plot.rss_gb == pytest.approx((5.0, 4.0, 9.0))


def test_read_window_never_exceeds_max_points(tmp_path):
    rows = [
        (2.0, 1000 + row_index, 1_000_000_000, 1.0)
        for row_index in range(1000)
    ]
    path = write_log(tmp_path, rows)
    plot = read_resource_window(path, TOTALS, 1000, 2000, 50)
    assert len(plot.times_s) <= 50


# A stop at ts 2000, then a restart at ts 3000 (elapsed drops 4 -> 2).
RESTART_ROWS = [
    (2.0, 1000, 5, 1.0),
    (4.0, 2000, 5, 1.0),
    (2.0, 3000, 5, 1.0),
    (4.0, 4000, 5, 1.0),
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
        sample(2.0, 1000, 5_000_000_000, 50.0),
        sample(4.0, 2000, 6_000_000_000, 60.0),
        sample(2.0, 3000, 1_000_000_000, 10.0),
    ]
    plot = get_resource_plot(samples, TOTALS, 0, 5000)
    assert plot.stop_times_s == pytest.approx((2.0,))
    assert plot.start_times_s == pytest.approx((3.0,))


def test_get_resource_plot_monotonic_has_no_restart():
    samples = [
        sample(elapsed, elapsed * 1000, 1, 1.0)
        for elapsed in (2.0, 4.0, 6.0, 8.0)
    ]
    plot = get_resource_plot(samples, TOTALS, 0, 100000)
    assert plot.stop_times_s == ()
    assert plot.start_times_s == ()


def test_get_resource_plot_keeps_only_windowed_samples():
    samples = [
        sample(elapsed, elapsed * 1000, 1, 1.0)
        for elapsed in (1.0, 2.0, 3.0, 4.0)
    ]
    plot = get_resource_plot(samples, TOTALS, 2000, 3000)
    assert plot.times_s == pytest.approx((2.0, 3.0))
