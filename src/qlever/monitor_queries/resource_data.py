"""Data layer for the server's resource-usage log.

Live tails the log into a rolling buffer; Historic reads a past window
straight off disk. Both turn samples into the immutable render models the
sparklines and the plot draw, so no widget does unit math of its own.
Getting the rows out of the file is `resource_reader.py`'s job.
"""

from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

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

LIVE_RESOURCE_WINDOW_MS = 300_000

# Intervals without a sample before the server counts as gone. Three, so
# a single missed sample still counts as live.
FRESH_INTERVALS = 3


@dataclass(frozen=True)
class Capacity:
    """What the machine has, so the bars and axes have a full scale.

    Read once at startup, since it cannot change while we run. `cores`
    is None when the count could not be read. `read_write_min_max_mbs`
    is not measured but set by the user (the `--read-write-min-max-mbs`
    option): the bounds of the disk read/write axis; None means that
    axis scales freely to its data.
    """

    ram_gb: float
    cores: float | None
    read_write_min_max_mbs: tuple[float, float] | None = None


class Column(NamedTuple):
    """One column of the resource log, and how to present it.

    `key` is the field on `Sample` and the key the series is stored
    under, so the log's own column name is the only name in play.
    `scale` divides a raw reading into display units. `reduce` says how
    several readings in one time bucket collapse into a point.
    `capacity` names the `Capacity` attribute that gives an axis its
    full height, and is None for a column with no ceiling. `floor` is
    the least full height the column's axis gets, so that readings
    which are small in absolute terms also look small; the axis still
    grows past it with the data. `bounds` names the `Capacity`
    attribute holding user-set (min, max) axis bounds, which replace
    the floor when they are set.
    """

    key: str
    label: str
    unit: str
    scale: float
    reduce: str
    capacity: str | None
    floor: float = 0.0
    bounds: str | None = None


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
        bounds="read_write_min_max_mbs",
    ),
    Column(
        key="write_bytes_per_s",
        label="write",
        unit="MB/s",
        scale=1e6,
        reduce="mean",
        capacity=None,
        bounds="read_write_min_max_mbs",
    ),
    # An I/O stall below 20% of the time is routine on a busy server,
    # so the floor keeps such readings low in the plot instead of
    # letting the axis magnify them; sustained values beyond that mean
    # real pressure and rise above.
    Column(
        key="io_stall_percent",
        label="io stall",
        unit="%",
        scale=1,
        reduce="mean",
        capacity=None,
        floor=20.0,
    ),
)


def buffer_size(sample_interval_s: int) -> int:
    """Samples the live window holds at this logging interval."""
    return max(1, LIVE_RESOURCE_WINDOW_MS // (sample_interval_s * 1000))


class SampleBuffer:
    """Rolling buffer of the most recent resource samples.

    `maxlen` drops the oldest sample when a new one arrives, so the
    buffer always holds the last `LIVE_RESOURCE_WINDOW_MS` of readings.
    Its size follows the log's sampling interval, so the window stays
    five minutes whatever interval the server was started with.
    """

    def __init__(self, sample_interval_s: int) -> None:
        self.size = buffer_size(sample_interval_s)
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
    bounds = getattr(capacity, column.bounds) if column.bounds else None
    axis_min, axis_max = bounds if bounds is not None else (column.floor, None)
    return ResourceSeries(
        key=column.key,
        label=column.label,
        unit=column.unit,
        values=tuple(
            bucket_value(column, running[index], counts[index])
            for index in filled
        ),
        total=getattr(capacity, column.capacity) if column.capacity else None,
        axis_min=axis_min,
        axis_max=axis_max,
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


def window_for_samples(
    samples: Iterable[Sample],
    capacity: Capacity,
    start_ms: int,
    end_ms: int,
    buckets: int,
) -> ResourceWindow:
    """Bucket samples by time into one window the widgets can draw.

    Samples arrive oldest first and may fall outside the window: those
    count towards events, so a restart just off the edge is not lost,
    but they get no bucket. Only the buckets are kept, so memory
    follows `buckets` and not the number of samples.
    """
    buckets = max(1, buckets)
    bucket_span_ms = (end_ms - start_ms) / buckets
    if bucket_span_ms <= 0:
        bucket_span_ms = 1
    bucket_ts = [None] * buckets
    running = {column.key: [0.0] * buckets for column in COLUMNS}
    counts = {column.key: [0] * buckets for column in COLUMNS}
    tracker = EventTracker(start_ms, end_ms)

    for sample in samples:
        tracker.track(sample)
        if start_ms <= sample.ts_ms <= end_ms:
            index = int((sample.ts_ms - start_ms) / bucket_span_ms)
            if index >= buckets:
                index = buckets - 1
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

    filled = [
        index for index in range(buckets) if bucket_ts[index] is not None
    ]
    series = {}
    for column in COLUMNS:
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
    capacity: Capacity,
    start_ms: int,
    end_ms: int,
    buckets: int,
    should_cancel: Callable[[], bool] | None = None,
) -> ResourceWindow:
    """Read a past window off disk, straight into the buckets."""
    # No log yet: the server has not started, or resource logging is
    # off. Frame the window empty rather than fail the read.
    if not path.exists():
        return window_for_samples([], capacity, start_ms, end_ms, buckets)
    with open(path, "rb") as stream:
        return window_for_samples(
            iter_samples(stream, start_ms, end_ms, should_cancel),
            capacity,
            start_ms,
            end_ms,
            buckets,
        )
