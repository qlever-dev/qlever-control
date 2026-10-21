"""Builds one window of readings for the sparklines and the plot.

Resource samples and finished operations land on the same time grid, so
one plot can read them against each other. Getting the rows out of the
resource log is `resource_reader.py`'s job.
"""

from bisect import bisect_left
from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from math import ceil
from pathlib import Path
from typing import Literal, NamedTuple

from qlever.monitor_queries.log_reader import CompletedQuery
from qlever.monitor_queries.models import (
    ResourceEvent,
    ResourceSeries,
    ResourceWindow,
)
from qlever.monitor_queries.resource_reader import (
    REQUIRED_COLUMNS,
    Sample,
    iter_samples,
)

# Live keeps an hour of samples, matching the metrics history, and
# draws whichever part of it the window stepper asks for.
LIVE_RESOURCE_BUFFER_MS = 3_600_000

# The window sizes Live offers, none reaching past what the buffer holds.
LIVE_WINDOW_PRESETS = ("5m", "15m", "30m", "1h")

# Intervals without a sample before the server counts as gone. Three, so
# a single missed sample still counts as live.
FRESH_INTERVALS = 3


@dataclass(frozen=True)
class Capacity:
    """What the machine has, so the bars and axes have a full scale.

    Read once at startup, since it cannot change while we run. `cores`
    is None when the count could not be read.
    """

    ram_gb: float
    cores: float | None


class Column(NamedTuple):
    """One series the plot can draw, and how to present it.

    `key` is the key the series is stored under, and for a resource
    column it is also the field on `Sample`, so the log's own column
    name is the only name in play.
    `scale` divides a raw reading into display units. `reduce` says how
    several readings in one time bucket collapse into a point.
    `capacity` names the `Capacity` attribute that gives an axis its
    full height, and is None for a column with no ceiling.
    """

    key: str
    label: str
    unit: str
    scale: float
    reduce: Literal["peak", "mean"]
    capacity: str | None


# One row per column, in the log's order. Everything that turns
# samples into a plot reads this table, so adding a column to the log
# means adding a row here and nothing else.
COLUMNS = (
    Column(
        key="rss",
        label="RSS",
        unit="GB",
        scale=1e9,
        reduce="peak",
        capacity="ram_gb",
    ),
    Column(
        key="cpu_percent",
        label="CPU",
        unit="cores",
        scale=100,
        reduce="peak",
        capacity="cores",
    ),
    Column(
        key="read_bytes_per_s",
        label="read",
        unit="MB/s",
        scale=1e6,
        reduce="mean",
        capacity=None,
    ),
    Column(
        key="write_bytes_per_s",
        label="write",
        unit="MB/s",
        scale=1e6,
        reduce="mean",
        capacity=None,
    ),
    Column(
        key="io_stall_percent",
        label="io stall",
        unit="%",
        scale=1,
        reduce="mean",
        capacity=None,
    ),
)

# One row per operation type, keyed by the `type` the metrics log
# writes. An operation with no type, or one this table does not know,
# misses the lookup and is left out. The log already reports
# milliseconds, so nothing is scaled.
OPERATION_COLUMNS = {
    "query": Column(
        key="query_mean_ms",
        label="query mean",
        unit="ms",
        scale=1,
        reduce="mean",
        capacity=None,
    ),
    "update": Column(
        key="update_mean_ms",
        label="update mean",
        unit="ms",
        scale=1,
        reduce="mean",
        capacity=None,
    ),
}

# Every series a window can hold, whichever log it came from.
ALL_COLUMNS = COLUMNS + tuple(OPERATION_COLUMNS.values())

# The series that come from the metrics log, so a plot needing one is
# gated on that log and not on the resource log's format.
OPERATION_KEYS = frozenset(column.key for column in OPERATION_COLUMNS.values())


def sample_count(span_ms: int, sample_interval_s: int) -> int:
    """Number of samples logged over `span_ms` at this interval."""
    return max(1, span_ms // (sample_interval_s * 1000))


class SampleBuffer:
    """Rolling buffer of the most recent resource samples.

    `maxlen` drops the oldest sample when a new one arrives. The size
    follows the log's sampling interval, so the buffer always reaches
    far enough back to fill Live's widest window.
    """

    def __init__(self, sample_interval_s: int) -> None:
        span_ms = live_buffer_span_ms(sample_interval_s)
        # One more sample than fit in the span, since n samples are only
        # n - 1 intervals apart.
        self.size = sample_count(span_ms, sample_interval_s) + 1
        self.samples = deque(maxlen=self.size)

    def add(self, sample: Sample) -> None:
        self.samples.append(sample)


def is_sample_fresh(
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


def coverage_note(window: ResourceWindow, sample_interval_s: int) -> str:
    """How far back the readings reach, when they miss the window start.

    Empty when they cover the window, so nothing is said in the common
    case. A gap shorter than one sampling interval is no gap at all,
    since no reading could have landed in it.
    """
    if not window.times_s:
        return ""
    if window.times_s[0] - window.start_s < sample_interval_s:
        return ""
    age_s = window.end_s - window.times_s[0]
    age = f"{int(age_s)}s" if age_s < 60 else f"{int(age_s // 60)}m"
    return f"{age} of data"


# The most buckets a window is split into, enough to fill a full-screen
# plot on a wide monitor and few enough to rebuild on every sample.
MAX_BUCKETS = 600


def bucket_width_ms(span_ms: int, sample_interval_s: int) -> int:
    """How wide each bucket of a window this long should be.

    A whole number of sampling intervals, so every full bucket holds the
    same number of samples and each point is made the same way.
    """
    interval_ms = sample_interval_s * 1000
    return interval_ms * ceil(span_ms / (MAX_BUCKETS * interval_ms))


def bucket_count(span_ms: int, sample_interval_s: int) -> int:
    """How many buckets of `bucket_width_ms` cover a window this long."""
    return ceil(span_ms / bucket_width_ms(span_ms, sample_interval_s))


def live_window_bounds(
    now_ms: int, span_ms: int, sample_interval_s: int
) -> tuple[int, int, int]:
    """Start, end and bucket count of Live's window, on the clock's grid.

    A bucket holds a whole number of sampling intervals, so every full
    bucket holds the same number of samples. Its edges are multiples of
    its width, so a full bucket never changes and the window moves on
    one whole bucket at a time.
    """
    bucket_ms = bucket_width_ms(span_ms, sample_interval_s)
    end_ms = (now_ms // bucket_ms + 1) * bucket_ms
    buckets = ceil(span_ms / bucket_ms)
    return end_ms - buckets * bucket_ms, end_ms, buckets


def live_buffer_span_ms(sample_interval_s: int) -> int:
    """How far back Live's samples must reach to fill its widest window.

    That window holds whole buckets, so it can be a little longer than
    `LIVE_RESOURCE_BUFFER_MS`. Its length does not depend on the time.
    """
    start_ms, end_ms, _ = live_window_bounds(
        0, LIVE_RESOURCE_BUFFER_MS, sample_interval_s
    )
    return end_ms - start_ms


def bucket_value(column: Column, running: float, count: int) -> float:
    """Collapse one bucket's readings into a single display value.

    A bucket with no reading gets a NaN rather than a zero, because the
    column was silent there and not idle. The plot draws a gap.
    """
    if count == 0:
        return float("nan")
    if column.reduce == "peak":
        return running / column.scale
    if column.reduce == "mean":
        return running / count / column.scale
    raise ValueError(f"{column.key} has an unknown reducer {column.reduce}")


def series_for_column(
    column: Column,
    capacity: Capacity,
    running: list[float],
    counts: list[int],
    filled: list[int],
) -> ResourceSeries:
    """Build one column's series from the buckets that had a sample.

    `running` holds a maximum per bucket for a peak column and a sum
    for a mean one. `filled` names the non-empty buckets and is shared
    by every column, so the series all line up with the window's
    `times_s`.
    """
    return ResourceSeries(
        key=column.key,
        label=column.label,
        unit=column.unit,
        values=tuple(
            bucket_value(column, running[index], counts[index])
            for index in filled
        ),
        total=getattr(capacity, column.capacity) if column.capacity else None,
    )


class EventTracker:
    """Finds server restarts and index rebuilds while walking samples.

    Both show up as a change between one sample and the next, so one
    pass in timestamp order finds them all. A restart is a drop in
    `elapsed_s`, which resets when the server starts. A rebuild
    boundary is a change in `index_rebuild_id`, which is empty when no
    rebuild is running.

    An event is kept only if its own time falls in the window, so a
    restart that straddles an edge still records the half that shows.
    """

    def __init__(self, start_ms: int, end_ms: int) -> None:
        self.start_ms = start_ms
        self.end_ms = end_ms
        self.events = []
        self.last = None

    def track(self, sample: Sample) -> None:
        """Compare one sample against the one before and note any event."""
        previous = self.last
        self.last = sample
        if previous is None:
            return
        restarted = sample.elapsed_s < previous.elapsed_s
        rebuild_changed = sample.index_rebuild_id != previous.index_rebuild_id
        # Events at the earlier sample come first, so the list stays in
        # time order without sorting it.
        if restarted:
            self.add("server_down", previous.ts_ms)
        if rebuild_changed and previous.index_rebuild_id is not None:
            self.add("rebuild_end", previous.ts_ms)
        if restarted:
            self.add("server_up", sample.ts_ms)
        if rebuild_changed and sample.index_rebuild_id is not None:
            self.add("rebuild_start", sample.ts_ms)

    def add(self, kind: str, ts_ms: int) -> None:
        """Record an event, unless it falls outside the window."""
        if self.start_ms <= ts_ms <= self.end_ms:
            self.events.append(ResourceEvent(kind=kind, time_s=ts_ms / 1000))


def bucket_index(
    ts_ms: int, start_ms: int, bucket_span_ms: float, buckets: int
) -> int:
    """Which bucket a timestamp inside the window falls in.

    A timestamp landing exactly on the window's end would index one
    past the last bucket, so it is pulled back into it.
    """
    index = int((ts_ms - start_ms) / bucket_span_ms)
    return min(index, buckets - 1)


# How far an operation may move to reach a bucket with a sample.
NEAREST_SAMPLE_MS = 5_000


def window_for_samples(
    samples: Iterable[Sample],
    operations: Iterable[CompletedQuery],
    capacity: Capacity,
    start_ms: int,
    end_ms: int,
    buckets: int,
) -> ResourceWindow:
    """Bucket samples and operations into one window the widgets draw.

    Samples arrive oldest first and may fall outside the window: those
    count towards events, so a restart just off the edge is not lost,
    but they get no bucket. An operation goes in the bucket its end
    time falls in. If that bucket has no sample, it moves to the nearest
    one that does, unless that is more than `NEAREST_SAMPLE_MS` away.
    Only the buckets are kept, so memory follows `buckets` and not the
    number of readings.
    """
    buckets = max(1, buckets)
    bucket_span_ms = (end_ms - start_ms) / buckets
    if bucket_span_ms <= 0:
        bucket_span_ms = 1
    bucket_ts = [None] * buckets
    running = {column.key: [0.0] * buckets for column in ALL_COLUMNS}
    counts = {column.key: [0] * buckets for column in ALL_COLUMNS}
    tracker = EventTracker(start_ms, end_ms)

    for sample in samples:
        tracker.track(sample)
        if start_ms <= sample.ts_ms <= end_ms:
            index = bucket_index(
                sample.ts_ms, start_ms, bucket_span_ms, buckets
            )
            if bucket_ts[index] is None:
                bucket_ts[index] = sample.ts_ms
            for column in COLUMNS:
                raw = getattr(sample, column.key)
                if raw is not None:
                    if column.reduce == "peak":
                        running[column.key][index] = max(
                            running[column.key][index], raw
                        )
                    elif column.reduce == "mean":
                        running[column.key][index] += raw
                    counts[column.key][index] += 1

    # The buckets holding a sample, which are the points the plot draws.
    filled = [
        index for index in range(buckets) if bucket_ts[index] is not None
    ]
    for operation in operations:
        column = OPERATION_COLUMNS.get(operation.op_type)
        if column is not None and start_ms <= operation.end_ms <= end_ms:
            index = bucket_index(
                operation.end_ms, start_ms, bucket_span_ms, buckets
            )
            if bucket_ts[index] is None:
                # No sample here, so use the nearest bucket that has one.
                after = bisect_left(filled, index)
                nearby = filled[max(0, after - 1) : after + 1]
                if not nearby:
                    continue
                index = min(
                    nearby,
                    key=lambda near: abs(bucket_ts[near] - operation.end_ms),
                )
                moved_ms = abs(bucket_ts[index] - operation.end_ms)
                if moved_ms > NEAREST_SAMPLE_MS:
                    continue
            running[column.key][index] += operation.duration_ms
            counts[column.key][index] += 1

    series = {}
    for column in ALL_COLUMNS:
        # The required columns are in every version of the log, so
        # their series exists even before a sample arrives. An optional
        # column appears only once one is actually seen.
        if any(counts[column.key]) or column.key in REQUIRED_COLUMNS:
            series[column.key] = series_for_column(
                column,
                capacity,
                running[column.key],
                counts[column.key],
                filled,
            )
    return ResourceWindow(
        start_s=start_ms / 1000,
        end_s=end_ms / 1000,
        times_s=tuple(bucket_ts[index] / 1000 for index in filled),
        series=series,
        events=tuple(tracker.events),
    )


def read_resource_window(
    path: Path,
    operations: Iterable[CompletedQuery],
    capacity: Capacity,
    start_ms: int,
    end_ms: int,
    buckets: int,
    should_cancel: Callable[[], bool] | None = None,
) -> ResourceWindow:
    """Read a past window off disk, straight into the buckets.

    The caller has already read the operations, so only the resource
    log is read here.
    """
    # No log yet: the server has not started, or resource logging is
    # off. Frame the window empty rather than fail the read.
    if not path.exists():
        return window_for_samples(
            [], operations, capacity, start_ms, end_ms, buckets
        )
    with open(path, "rb") as stream:
        return window_for_samples(
            iter_samples(stream, start_ms, end_ms, should_cancel),
            operations,
            capacity,
            start_ms,
            end_ms,
            buckets,
        )
