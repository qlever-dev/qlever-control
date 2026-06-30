"""Data layer for the Live screen.

Owns the in-memory state the Live screen renders: the queries currently
running on the server and the rolling 1h history of completed queries
that feeds the 5m, 15m, and 1h metric rows. The tailer is the only
steady-state writer; the screen's refresh callbacks read under the same
lock and return the frozen `models.py` dataclasses the widgets consume.
"""

import json
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

from qlever.monitor_queries.log_reader import (
    CompletedQuery,
    load_sparql_at,
    normalize_status,
    offset_for_ts,
    open_log_buffer,
    pair_start_end_events,
    read_last_timestamp,
    scan_range,
)
from qlever.monitor_queries.metrics import (
    EMPTY_FIELDS,
    MetricsSnapshot,
    metrics_for_ranges,
)
from qlever.monitor_queries.models import LiveQueryRow, MetricsCounts

LIVE_METRIC_WINDOWS_MS = [1 * 60_000, 5 * 60_000, 60 * 60_000]
LIVE_HORIZON_MS = LIVE_METRIC_WINDOWS_MS[2]
LIVE_METRIC_LABELS = ["last 1m", "last 5m", "last 1h"]
LOG_IDLE_THRESHOLD_MS = 10_000
PING_INTERVAL_S = 5.0
PING_FAILS_TO_UNREACHABLE = 3


def current_ms() -> int:
    """Wall-clock now in epoch milliseconds."""
    return time.time_ns() // 1_000_000


def is_log_fresh(state: "LiveState", now_ms: int) -> bool:
    """True iff the log produced a line within LOG_IDLE_THRESHOLD_MS.

    The live screen's reachability state machine uses this as the
    cheap evidence stream; while it's True we skip pinging the server.
    """
    last_ms = state.latest_event_ms
    return last_ms is not None and now_ms - last_ms <= LOG_IDLE_THRESHOLD_MS


class CompletedQueries:
    """Rolling 1h history of completed queries.

    Storage is a deque ordered by insertion, which is the tailer's
    forward read order. Wall-clock is monotonic in steady state so
    older entries sit at the left and trimming is one popleft loop.
    """

    def __init__(self) -> None:
        self.entries = deque()

    def add(self, query: CompletedQuery) -> None:
        """Append a completed query to the history.

        Caller writes in tailer order, which is wall-clock order in
        steady state; entries remain sorted by end_ms for trimming.
        """
        self.entries.append(query)

    def drop_older_than(self, cutoff_ms: int) -> None:
        """Discard completions that ended before cutoff_ms.

        Called on each tailer tick with cutoff_ms = now_ms - 1h so the
        history stays bounded even during quiet periods.
        """
        while self.entries and self.entries[0].end_ms < cutoff_ms:
            self.entries.popleft()

    def metrics_for_windows(
        self,
        now_ms: int,
        windows_ms: list[int],
        slow_threshold_ms: int,
        data_start_ms: int,
    ) -> list[MetricsSnapshot | None]:
        """Compute one snapshot per window in a single deque walk.

        Each window is the interval [now_ms - width, now_ms].
        data_start_ms is the oldest timestamp the caller has loaded
        complete data for; a window starting before that gets None
        instead of a snapshot, so the widget can render the row as
        not-yet-ready. Returns snapshots in the same order as windows_ms.
        """
        ranges = [(now_ms - width, now_ms) for width in windows_ms]
        snapshots = metrics_for_ranges(self.entries, ranges, slow_threshold_ms)
        return [
            None if lo_ms < data_start_ms else snapshot
            for (lo_ms, _), snapshot in zip(ranges, snapshots)
        ]


@dataclass
class ActiveQuery:
    """A running or just-finished query on the Live screen.

    end_ms is None while the query runs; seen is set once a repaint has
    rendered the query, so one too fast to catch while running still
    shows for a single paint before being dropped.
    """

    start_ms: int
    end_ms: int | None
    client_ip: str
    sparql: str
    seen: bool = False


@dataclass
class LiveState:
    """Shared in-memory state for the Live screen.

    The tailer is the steady-state writer; the boot-time loader prepends
    historical completions once. UI refresh callbacks read under the
    same lock and build metric rows on the fly.
    """

    completed: CompletedQueries = field(default_factory=CompletedQueries)
    active: dict[str, ActiveQuery] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)
    metrics_known_from_ms: int | None = None
    latest_event_ms: int | None = None


def find_active_queries(
    log_path: Path,
    window_pad_ms: int,
) -> tuple[LiveState, int, int]:
    """Scan the log's tail to determine queries still running.

    Reads [eof_ts - window_pad_ms, EOF], pairs start/end events, and
    records the unmatched starts as the active set. The completed
    pairs found in this window are discarded; backfill rediscovers
    them. Loads SPARQL text for each survivor by stored offset.

    Returns (state, cut_offset, eof_ts). cut_offset is where the tailer
    starts reading forward; eof_ts is the timestamp of the log's last
    complete line, or 0 when the log holds none.
    """
    state = LiveState()
    with open_log_buffer(log_path) as buf:
        if buf is None:
            return (state, 0, 0)
        file_size = len(buf)
        eof_ts = read_last_timestamp(buf)
        if eof_ts is None:
            return (state, file_size, 0)

        state.latest_event_ms = eof_ts
        lo_offset = offset_for_ts(buf, eof_ts - window_pad_ms)
        events = scan_range(buf, lo_offset, file_size)
        _, still_open = pair_start_end_events(events)

        for qid, (start_ms, start_line_offset) in still_open.items():
            _, client_ip, sparql = load_sparql_at(buf, start_line_offset)
            state.active[qid] = ActiveQuery(
                start_ms=start_ms,
                end_ms=None,
                client_ip=client_ip,
                sparql=sparql,
            )

    return (state, file_size, eof_ts)


def load_completed_history(
    log_path: Path,
    state: LiveState,
    cut_offset: int,
    window_pad_ms: int,
    now_ms: Callable[[], int] = current_ms,
) -> None:
    """Scan the hour before cut_offset and seed completed history.

    One-shot: callers spawn this on a daemon thread at startup if they
    want it off the main loop. After it returns, the tailer is the only
    writer of state.completed.

    The scan starts window_pad_ms before the hour boundary so a query
    that began before the hour but ended inside it still has its start
    line in range and can be paired. Pairs that ended before the hour
    are then dropped, matching the deque's end_ms retention.
    """
    with open_log_buffer(log_path) as buf:
        if buf is None:
            return
        oldest_wanted_ms = now_ms() - LIVE_HORIZON_MS
        scan_start_offset = offset_for_ts(
            buf, oldest_wanted_ms - window_pad_ms
        )
        events = scan_range(buf, scan_start_offset, cut_offset)
        paired, _ = pair_start_end_events(events)
        older_completions = [
            query for query in paired if query.end_ms >= oldest_wanted_ms
        ]

    with state.lock:
        tailer_entries = state.completed.entries
        state.completed.entries = deque(older_completions)
        state.completed.entries.extend(tailer_entries)


class LiveLogReader:
    """Tail cursor and per-poll logic for the live query log.

    Sole writer of LiveState in steady state. Each poll() reads any
    whole lines appended since the last cursor, dispatches them into
    active / completed under the state lock, and trims stale entries.
    The run loop is owned by the caller.
    """

    def __init__(
        self,
        log_path: Path,
        state: LiveState,
        cut_offset: int,
        window_pad_ms: int,
        poll_interval: float = 0.2,
        now_ms: Callable[[], int] = current_ms,
    ) -> None:
        self.log_path = log_path
        self.state = state
        self.cursor = cut_offset
        self.window_pad_ms = window_pad_ms
        self.poll_interval = poll_interval
        self.now_ms = now_ms

    def poll(self, log_stream: BinaryIO) -> None:
        """Process any whole lines appended since the last poll."""
        log_stream.seek(self.cursor)
        for line in log_stream:
            if not line.endswith(b"\n"):
                break
            self.cursor += len(line)
            self.handle_line(line)
        self.evict_stale()

    def evict_stale(self) -> None:
        """Drop old completions and finished or stale active entries.

        An active entry leaves once a repaint has shown it and it has
        finished, or once its start is older than window_pad_ms, which
        means its end event was missed since no real query runs that
        long.
        """
        now = self.now_ms()
        completed_cutoff = now - LIVE_HORIZON_MS
        active_cutoff = now - self.window_pad_ms
        with self.state.lock:
            self.state.completed.drop_older_than(completed_cutoff)
            to_remove = [
                qid
                for qid, entry in self.state.active.items()
                if (entry.end_ms is not None and entry.seen)
                or entry.start_ms < active_cutoff
            ]
            for qid in to_remove:
                del self.state.active[qid]

    def handle_line(self, line: bytes) -> None:
        """Parse one whole line and dispatch it into LiveState."""
        try:
            obj = json.loads(line)
        except (ValueError, TypeError):
            return
        if not isinstance(obj, dict):
            return
        ts_ms = obj.get("ts-ms")
        event = obj.get("event")
        qid = obj.get("qid")
        if not isinstance(ts_ms, int) or not isinstance(qid, str):
            return

        if event == "start":
            sparql = obj.get("query")
            if not isinstance(sparql, str):
                sparql = ""
            client_ip = obj.get("client-ip", "")
            with self.state.lock:
                self.state.active[qid] = ActiveQuery(
                    start_ms=ts_ms,
                    end_ms=None,
                    client_ip=client_ip,
                    sparql=sparql,
                )
                self.state.latest_event_ms = max(
                    self.state.latest_event_ms or 0, ts_ms
                )
            return

        if event == "end":
            status = obj.get("status")
            if not isinstance(status, str):
                return
            with self.state.lock:
                self.state.latest_event_ms = max(
                    self.state.latest_event_ms or 0, ts_ms
                )
                entry = self.state.active.get(qid)
                if entry is None:
                    return
                entry.end_ms = ts_ms
                self.state.completed.add(
                    CompletedQuery(
                        start_ms=entry.start_ms,
                        end_ms=ts_ms,
                        duration_ms=ts_ms - entry.start_ms,
                        status=normalize_status(status),
                        start_line_offset=None,
                    )
                )


def get_live_query_rows(state: LiveState, now_ms: int) -> list[LiveQueryRow]:
    """Snapshot the active set as table rows, marking each one shown.

    Skips entries that have finished and already appeared in a repaint
    (they linger only until the tailer reaps them), and flips seen on
    every returned entry. A query that finished before any paint could
    catch it running is therefore still shown exactly once. The caller
    chooses the clock: the Live screen passes display_clock_ms() so
    freeze-on-unreachable freezes durations too.
    """
    rows = []
    with state.lock:
        for qid, entry in state.active.items():
            if entry.end_ms is not None and entry.seen:
                continue
            entry.seen = True
            duration_ms = (
                entry.end_ms - entry.start_ms
                if entry.end_ms is not None
                else now_ms - entry.start_ms
            )
            rows.append(
                LiveQueryRow(
                    qid=qid,
                    started_at_ms=entry.start_ms,
                    duration_ms=duration_ms,
                    sparql=entry.sparql,
                    client_ip=entry.client_ip,
                )
            )
    return rows


def discard_finished_backlog(state: LiveState) -> None:
    """Mark finished-but-unshown active entries as seen.

    Called on screen resume so the queries that finished while the user
    was away are dropped instead of all flashing at once. Running
    entries are left to show.
    """
    with state.lock:
        for entry in state.active.values():
            if entry.end_ms is not None:
                entry.seen = True


def format_eta(ms: int) -> str:
    """Coarse countdown for a not-ready metric row; sub-minute reads "<1m"."""
    if ms < 60_000:
        return "<1m"
    return f"{ms // 60_000}m"


def get_live_metrics(
    state: LiveState,
    slow_threshold_ms: int,
    now_ms: int,
) -> list[MetricsCounts]:
    """Three rolling-window metric rows for the Live screen.

    Takes a short-locked deque copy so the metric scan and percentile
    work runs lock-free while the tailer keeps appending. A window
    whose start predates metrics_known_from_ms renders as an all-None
    row so the widget shows "..." until coverage reaches that range.
    """
    with state.lock:
        history = CompletedQueries()
        history.entries = deque(state.completed.entries)
        coverage_start_ms = state.metrics_known_from_ms
    snapshots = history.metrics_for_windows(
        now_ms=now_ms,
        windows_ms=LIVE_METRIC_WINDOWS_MS,
        slow_threshold_ms=slow_threshold_ms,
        data_start_ms=coverage_start_ms or now_ms,
    )
    rows = []
    for label, width_ms, snap in zip(
        LIVE_METRIC_LABELS, LIVE_METRIC_WINDOWS_MS, snapshots
    ):
        # Show "ready in Nm" only once we know when coverage arrives;
        # during boot ramp coverage_start_ms is None and rows just read "…".
        message = None
        if snap is None and coverage_start_ms is not None:
            eta_ms = coverage_start_ms + width_ms - now_ms
            message = f"ready in {format_eta(eta_ms)}"
        rows.append(
            MetricsCounts(
                label=label,
                **(snap._asdict() if snap is not None else EMPTY_FIELDS),
                not_ready_message=message,
            )
        )
    return rows
