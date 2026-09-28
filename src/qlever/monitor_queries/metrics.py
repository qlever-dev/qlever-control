"""Summarises a group of finished queries into one metrics row.

The Live and Historic screens both display these counts and timings.
Keeping the math here lets both engines share it without importing
each other.
"""

import statistics
from collections.abc import Collection, Iterable
from typing import NamedTuple

from qlever.monitor_queries.log_reader import CompletedQuery


class MetricsSnapshot(NamedTuple):
    """One metrics row: counts and timings for a group of finished queries.

    Every finished query has exactly one status, so the status counts
    always add up to seen:
        seen == ok + failed + timeout + cancelled + unknown
    slow is counted separately, since a query can be both ok and slow.
    am and gm are the arithmetic and geometric mean run times, p50 and
    p95 the median and 95th-percentile run times, all in milliseconds,
    or None when there are no queries to measure.
    """

    seen: int
    ok: int
    failed: int
    timeout: int
    cancelled: int
    unknown: int
    slow: int
    am: int | None
    gm: int | None
    p50: int | None
    p95: int | None


# All-None counts, spread into MetricsCounts when a row has no data yet.
EMPTY_FIELDS = dict.fromkeys(MetricsSnapshot._fields)


def percentiles(durations_ms: list[int]) -> tuple[int | None, int | None]:
    """Return the median (p50) and 95th-percentile (p95) run times.

    Both are in milliseconds. Returns (None, None) when the list is
    empty.
    """
    if not durations_ms:
        return (None, None)
    if len(durations_ms) == 1:
        only = durations_ms[0]
        return (only, only)
    cuts = statistics.quantiles(durations_ms, n=100)
    return (round(cuts[49]), round(cuts[94]))


def build_snapshot(
    counts: dict[str, int],
    slow: int,
    durations_ms: list[int],
) -> MetricsSnapshot:
    """Build one MetricsSnapshot from already-counted query results.

    counts holds the per-status totals, slow is how many queries ran
    longer than the slow threshold, and durations_ms is every query's
    run time (its length is the total seen).
    """
    p50, p95 = percentiles(durations_ms)
    am = None if not durations_ms else int(statistics.mean(durations_ms))
    # A single zero would collapse the geometric mean to zero, so each
    # run time is floored at 1ms.
    gm = None
    if durations_ms:
        gm = int(statistics.geometric_mean(max(ms, 1) for ms in durations_ms))
    return MetricsSnapshot(
        seen=len(durations_ms),
        ok=counts["ok"],
        failed=counts["failed"],
        timeout=counts["timeout"],
        cancelled=counts["cancelled"],
        unknown=counts["unknown"],
        slow=slow,
        am=am,
        gm=gm,
        p50=p50,
        p95=p95,
    )


def metrics_for_queries(
    completed: Iterable[CompletedQuery],
    slow_threshold_ms: int,
) -> MetricsSnapshot:
    """Compute the metrics snapshot for a set of completed queries."""
    counts = {"ok": 0, "failed": 0, "timeout": 0, "cancelled": 0, "unknown": 0}
    slow = 0
    durations = []
    for entry in completed:
        counts[entry.status] += 1
        if entry.duration_ms >= slow_threshold_ms:
            slow += 1
        durations.append(entry.duration_ms)
    return build_snapshot(counts, slow, durations)


def metrics_for_ranges(
    completed: Collection[CompletedQuery],
    ranges: list[tuple[int, int]],
    slow_threshold_ms: int,
) -> list[MetricsSnapshot]:
    """Summarise completed queries into one snapshot per time range.

    Each range is an absolute (lo_ms, hi_ms) interval; a completed
    query counts toward a range when its end_ms falls inside it.
    Returns one snapshot per range, in the order the ranges were given.
    """
    return [
        metrics_for_queries(
            [entry for entry in completed if lo_ms <= entry.end_ms <= hi_ms],
            slow_threshold_ms,
        )
        for lo_ms, hi_ms in ranges
    ]
