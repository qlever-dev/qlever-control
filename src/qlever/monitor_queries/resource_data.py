"""Data layer for the server's resource-usage log.

Live tails the log into a rolling buffer; Historic reads a past window
straight off disk. Both turn samples into the immutable render models the
sparklines and the plot draw, so no widget does unit math of its own.
"""

from collections import deque
from collections.abc import Callable
from pathlib import Path
from typing import BinaryIO

import psutil

from qlever.monitor_queries.models import (
    ResourcePlot,
    ResourceSample,
    ResourceSeries,
    ResourceTotals,
    ResourceUsage,
)

LIVE_WINDOW_S = 300

# Intervals without a sample before the server counts as gone. Three, so
# a single missed sample still counts as live.
FRESH_INTERVALS = 3

# Bytes to read from the tail per buffered row when seeding, a generous
# 64 so the whole window always fits however large the log grew.
SEED_BYTES_PER_ROW = 64

# The resource-usage log's columns in order. The optional columns can be empty
# and are only present in the new log format.
REQUIRED_COLUMNS = ("elapsed_s", "timestamp_ms", "rss", "cpu_percent")
OPTIONAL_COLUMNS = (
    "read_bytes_per_s",
    "write_bytes_per_s",
    "io_stall_percent",
    "rebuild_id",
)
LOG_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS


def log_has_new_columns(log_path: Path) -> bool:
    """Determine if the resource log file has the new optional columns."""
    try:
        with log_path.open() as log_file:
            header = log_file.readline()
    except OSError:
        return False
    return len(header.split("\t")) == len(LOG_COLUMNS)


def buffer_size(sample_interval_s: int) -> int:
    """Samples the live window holds at this logging interval."""
    return max(1, LIVE_WINDOW_S // sample_interval_s)


def system_totals() -> ResourceTotals:
    """Read the host's RAM and core count, the scales everything shows against."""
    return ResourceTotals(
        ram_gb=psutil.virtual_memory().total / 1e9,
        cores=psutil.cpu_count(),
    )


class ResourceHistory:
    """Rolling buffer of the most recent resource samples.

    maxlen makes it a ring: appending past the size drops the oldest, so
    it always holds the last LIVE_WINDOW_S seconds of readings. The size
    follows the log's sampling interval, so the window stays 5 minutes
    whatever interval the server was started with.
    """

    def __init__(self, sample_interval_s: int) -> None:
        self.size = buffer_size(sample_interval_s)
        self.samples = deque(maxlen=self.size)

    def add(self, sample: ResourceSample) -> None:
        self.samples.append(sample)


def optional_cell(
    text: str, convert: Callable[[str], float | int]
) -> float | int | None:
    """Convert one optional column cell, or None if empty"""
    text = text.strip()
    return convert(text) if text else None


def parse_tsv_row(line: str) -> ResourceSample | None:
    """Turn one TSV log row into a sample, or None if it isn't one.

    The header line and any malformed row fail the numeric parse and
    return None, so the caller never special-cases them.
    """
    fields = line.split("\t")
    if len(fields) == len(REQUIRED_COLUMNS):
        fields += [""] * len(OPTIONAL_COLUMNS)
    if len(fields) != len(LOG_COLUMNS):
        return None
    elapsed, ts, rss, cpu, read_bytes, write_bytes, io_stall, rebuild_id = (
        fields
    )
    try:
        return ResourceSample(
            elapsed_s=float(elapsed),
            ts_ms=int(ts),
            rss=int(rss),
            cpu_percent=float(cpu),
            read_bytes_per_s=optional_cell(read_bytes, float),
            write_bytes_per_s=optional_cell(write_bytes, float),
            io_stall_percent=optional_cell(io_stall, float),
            rebuild_id=optional_cell(rebuild_id, int),
        )
    except ValueError:
        return None


class ResourceLogReader:
    """Tail cursor for the server's resource-usage TSV, like LiveLogReader.

    The worker owns the open stream and passes it to each read. read_new
    returns only the rows appended since the last cursor, so the steady
    read never depends on file size. last_ts_ms is the freshest row's
    time, read by the Live screen as a fast 'server is alive' signal
    before the metrics-log and ping checks.
    """

    def __init__(self, buffered_rows: int) -> None:
        self.cursor = 0
        self.last_ts_ms = None
        self.tail_bytes = buffered_rows * SEED_BYTES_PER_ROW

    def seed(self, stream: BinaryIO, now_ms: int) -> list[ResourceSample]:
        """Backfill the last window from the tail of the log, once.

        Seeks near the end rather than scanning from the start, so a log
        grown large over past sessions costs a fixed read. Skips the
        partial line the seek lands in, then reads forward like a normal
        poll. The log is opened in append mode, so the tail can carry
        rows from an earlier session; the cutoff drops them so the buffer
        starts as a true LIVE_WINDOW_S window rather than stale history.
        """
        stream.seek(0, 2)
        start = max(0, stream.tell() - self.tail_bytes)
        stream.seek(start)
        if start > 0:
            stream.readline()
        self.cursor = stream.tell()
        cutoff = now_ms - LIVE_WINDOW_S * 1000
        return [
            sample
            for sample in self.read_new(stream)
            if sample.ts_ms >= cutoff
        ]

    def read_new(self, stream: BinaryIO) -> list[ResourceSample]:
        """Parse and return samples appended since the previous read.

        Stops at the first line without a trailing newline, leaving a
        half-written final row for the next read so a row is never split.
        """
        stream.seek(self.cursor)
        samples = []
        for line in stream:
            if not line.endswith(b"\n"):
                break
            self.cursor += len(line)
            sample = parse_tsv_row(line.decode())
            if sample is not None:
                samples.append(sample)
        if samples:
            self.last_ts_ms = samples[-1].ts_ms
        return samples


def is_resource_sample_fresh(
    last_ts_ms: int | None, now_ms: int, sample_interval_s: int
) -> bool:
    """Whether a sample is recent enough to prove the server is alive.

    Used only to promote to reachable; its absence is ambiguous (remote
    server, mount, wrong process) so it never forces unreachable.
    """
    if last_ts_ms is None:
        return False
    fresh_ms = FRESH_INTERVALS * sample_interval_s * 1000
    return now_ms - last_ts_ms <= fresh_ms


def zero_pad_left(values: tuple[float, ...], size: int) -> tuple[float, ...]:
    """Left-pad a partial window up to `size` with zeros.

    A not-yet-full buffer would otherwise stretch its few readings into
    fat bars. Fixing the slot count keeps the bar width constant, with
    the empty pre-monitoring past on the left and 'now' at the right.
    """
    missing = size - len(values)
    return (0.0,) * missing + values if missing > 0 else values


def get_resource_usage(
    history: ResourceHistory, totals: ResourceTotals
) -> ResourceUsage:
    """Snapshot the buffer as two display-ready sparkline series.

    Walks the buffer once, converting raw samples to display units: rss
    bytes to GB, cpu percent to cores.
    """
    rss_values = zero_pad_left(
        tuple(sample.rss / 1e9 for sample in history.samples), history.size
    )
    cpu_values = zero_pad_left(
        tuple(sample.cpu_percent / 100 for sample in history.samples),
        history.size,
    )
    return ResourceUsage(
        rss=ResourceSeries("RSS", rss_values, totals.ram_gb, "GB"),
        cpu=ResourceSeries("CPU", cpu_values, totals.cores, "cores"),
    )


class RestartTracker:
    """Finds server restarts from drops in the log's elapsed-time column.

    Fed samples in timestamp order. The elapsed-time counter resets when
    the server restarts, so a drop between two samples means a restart
    happened between them. The earlier sample is the stop, the later one
    the start, and each is kept only if it falls in the window, so a
    restart straddling a window edge still records the half that shows.
    """

    def __init__(self, start_ms: int, end_ms: int) -> None:
        self.start_ms = start_ms
        self.end_ms = end_ms
        self.stop_times_s = []
        self.start_times_s = []
        self.last_elapsed_s = None
        self.last_ts_ms = None

    def track(self, elapsed_s: float, ts_ms: int) -> None:
        """Note one sample; record a restart if elapsed time dropped."""
        if self.last_elapsed_s is not None and elapsed_s < self.last_elapsed_s:
            if self.start_ms <= self.last_ts_ms <= self.end_ms:
                self.stop_times_s.append(self.last_ts_ms / 1000)
            if self.start_ms <= ts_ms <= self.end_ms:
                self.start_times_s.append(ts_ms / 1000)
        self.last_elapsed_s = elapsed_s
        self.last_ts_ms = ts_ms


def build_plot(
    times_s: list[float],
    rss_gb: list[float],
    cpu_cores: list[float],
    totals: ResourceTotals,
    start_ms: int,
    end_ms: int,
    restarts: RestartTracker,
) -> ResourcePlot:
    """Assemble a ResourcePlot from gathered series, window, and restarts."""
    return ResourcePlot(
        times_s=tuple(times_s),
        rss_gb=tuple(rss_gb),
        cpu_cores=tuple(cpu_cores),
        rss_total=totals.ram_gb,
        cpu_total=totals.cores,
        start_s=start_ms / 1000,
        end_s=end_ms / 1000,
        stop_times_s=tuple(restarts.stop_times_s),
        start_times_s=tuple(restarts.start_times_s),
    )


def get_resource_plot(
    samples: list[ResourceSample],
    totals: ResourceTotals,
    start_ms: int,
    end_ms: int,
) -> ResourcePlot:
    """Turn samples in a time window into the dual-axis plot model.

    Keeps only samples inside [start_ms, end_ms] and converts each to
    display units: rss bytes to GB, cpu percent to cores. The window
    edges frame the plot's x-axis and may be wider than the samples that
    fall inside them. Restarts are detected by RestartTracker.
    """
    times_s = []
    rss_gb = []
    cpu_cores_series = []
    restarts = RestartTracker(start_ms, end_ms)
    for sample in samples:
        restarts.track(sample.elapsed_s, sample.ts_ms)
        if start_ms <= sample.ts_ms <= end_ms:
            times_s.append(sample.ts_ms / 1000)
            rss_gb.append(sample.rss / 1e9)
            cpu_cores_series.append(sample.cpu_percent / 100)
    return build_plot(
        times_s, rss_gb, cpu_cores_series, totals, start_ms, end_ms, restarts
    )


# Backup before the bisect's landing offset, so the forward scan never
# skips the boundary line when it lands exactly on a line start.
SEEK_BACKUP_BYTES = 256


def line_ts_ms(line: bytes) -> int | None:
    """Read the timestamp (column 1) from a raw TSV line, or None.

    The header row and a partial line fail the int parse and return
    None, so the bisect treats them as before the window.
    """
    parts = line.split(b"\t")
    if len(parts) < 2:
        return None
    try:
        return int(parts[1])
    except ValueError:
        return None


def seek_to_window_start(
    stream: BinaryIO, start_ms: int, file_size: int
) -> int:
    """Bisect for the byte offset whose next full line is at or after start_ms.

    The log is time-ordered, so "the line after mid is at or after
    start_ms" is monotonic in mid. Returns that boundary offset; the
    caller backs up a little before reading so the boundary line is
    never skipped.
    """
    lo, hi = 0, file_size
    while lo < hi:
        mid = (lo + hi) // 2
        stream.seek(mid)
        stream.readline()
        ts = line_ts_ms(stream.readline())
        if ts is not None and ts >= start_ms:
            hi = mid
        else:
            lo = mid + 1
    return lo


# Rows scanned between should_cancel polls, so a long read can abort
# without the check itself costing anything on the common short read.
CANCEL_CHECK_ROWS = 50_000


def read_resource_window(
    path: Path,
    totals: ResourceTotals,
    start_ms: int,
    end_ms: int,
    max_points: int,
    should_cancel: Callable[[], bool] | None = None,
) -> ResourcePlot:
    """Read samples in [start_ms, end_ms] and bucket them for the plot.

    Seeks near the window start, then streams forward, folding each row
    into one of max_points equal time buckets and keeping the bucket's
    peak rss and cpu so spikes survive. Restarts are detected by
    RestartTracker.
    should_cancel is polled while scanning so a long read can abort.
    Memory stays at O(max_points) however large the log is.
    """
    # No log yet: the server has not started, or resource logging is
    # off. Frame the window empty rather than fail the read.
    if not path.exists():
        return get_resource_plot([], totals, start_ms, end_ms)
    max_points = max(1, max_points)
    bucket_span_ms = (end_ms - start_ms) / max_points
    if bucket_span_ms <= 0:
        bucket_span_ms = 1
    bucket_ts = [None] * max_points
    bucket_rss = [0] * max_points
    bucket_cpu = [0.0] * max_points
    restarts = RestartTracker(start_ms, end_ms)
    rows_since_check = 0

    with open(path, "rb") as stream:
        stream.seek(0, 2)
        file_size = stream.tell()
        boundary = seek_to_window_start(stream, start_ms, file_size)
        read_from = max(0, boundary - SEEK_BACKUP_BYTES)
        stream.seek(read_from)
        if read_from > 0:
            stream.readline()
        for raw in stream:
            rows_since_check += 1
            if (
                should_cancel is not None
                and rows_since_check >= CANCEL_CHECK_ROWS
            ):
                if should_cancel():
                    break
                rows_since_check = 0
            sample = parse_tsv_row(raw.decode())
            if sample is None:
                continue
            restarts.track(sample.elapsed_s, sample.ts_ms)
            # Break only after tracking, so the first row past the window
            # still pairs with an in-window stop.
            if sample.ts_ms > end_ms:
                break
            if sample.ts_ms < start_ms:
                continue
            index = int((sample.ts_ms - start_ms) / bucket_span_ms)
            if index >= max_points:
                index = max_points - 1
            if bucket_ts[index] is None:
                bucket_ts[index] = sample.ts_ms
            bucket_rss[index] = max(bucket_rss[index], sample.rss)
            bucket_cpu[index] = max(bucket_cpu[index], sample.cpu_percent)

    times_s = []
    rss_gb = []
    cpu_cores_series = []
    for index in range(max_points):
        if bucket_ts[index] is not None:
            times_s.append(bucket_ts[index] / 1000)
            rss_gb.append(bucket_rss[index] / 1e9)
            cpu_cores_series.append(bucket_cpu[index] / 100)
    return build_plot(
        times_s, rss_gb, cpu_cores_series, totals, start_ms, end_ms, restarts
    )
