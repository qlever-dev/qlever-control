"""Data layer for the Historic screen.

Reads one time window of the log into a list of `LoggedQuery`,
filters it by display mode, and maps the survivors into the
`models.py` dataclasses the screen renders. Metrics are computed per
mode at render time. A window change reruns the scan; a mode change
reuses the scanned list.
"""

import json
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import NamedTuple

from qlever.monitor_queries.log_reader import (
    CLIENT_IP_KEY,
    HEAD_BYTES,
    CompletedQuery,
    line_query_contains,
    load_sparql_snippet_at,
    offset_for_ts,
    open_log_buffer,
    pair_start_end_events,
    scan_range,
    slice_string_value,
)
from qlever.monitor_queries.metrics import metrics_for_queries
from qlever.monitor_queries.models import (
    FilterState,
    HistoricQueryRow,
    MetricsCounts,
)

# Used as the duration of orphaned queries. Negative so they sort last.
DURATION_UNKNOWN = -1


class LoggedQuery(NamedTuple):
    """One SPARQL query observed in the log over the current window.

    `end_ms` is `None` when the query started but has not ended yet,
    either because it is still running or the server crashed before
    writing the end event. `status` carries the raw end status, or
    `"running"` for a still-open survivor. `start_line_offset` is the
    byte offset of the start line, used to read the query's text only
    once it is about to be displayed.
    """

    start_ms: int
    end_ms: int | None
    status: str
    start_line_offset: int


def read_window(
    log_path: Path,
    window_start_ms: int,
    window_end_ms: int,
    pad_ms: int,
    log_end_ms: int,
    now_ms: int,
    should_cancel: Callable[[], bool] | None = None,
) -> list[LoggedQuery]:
    """Scan one time window of the log into the queries overlapping it.

    The byte range scanned is the window padded by `pad_ms` on each
    side so both events of a query straddling the window edge are
    recovered. Pairs that lie entirely inside the pad are dropped, so
    the result holds only rows a mode predicate could keep. The query
    text is not read here; `load_query_details_for_rows` reads it for
    the visible rows. Metrics are computed later, per mode, by
    `window_metrics`.

    A still-open query is `"running"` only if its start is within
    `pad_ms` of `log_end_ms` and the log itself is fresh
    (`now_ms - log_end_ms <= pad_ms`); otherwise `"orphaned"`.
    """
    with open_log_buffer(log_path) as buf:
        if buf is None:
            return []
        lo_offset = offset_for_ts(buf, window_start_ms - pad_ms)
        hi_bound = offset_for_ts(buf, window_end_ms + pad_ms)
        events = scan_range(buf, lo_offset, hi_bound, should_cancel)
        completed, still_open = pair_start_end_events(events)

        queries = []
        for pair in completed:
            if pair.start_ms > window_end_ms or pair.end_ms < window_start_ms:
                continue
            queries.append(
                LoggedQuery(
                    start_ms=pair.start_ms,
                    end_ms=pair.end_ms,
                    status=pair.status,
                    start_line_offset=pair.start_line_offset,
                )
            )
        log_is_fresh = now_ms - log_end_ms <= pad_ms
        running_cutoff_ms = log_end_ms - pad_ms
        for start_ms, start_line_offset in still_open.values():
            if start_ms > window_end_ms:
                continue
            status = (
                "running"
                if log_is_fresh and start_ms >= running_cutoff_ms
                else "orphaned"
            )
            queries.append(
                LoggedQuery(
                    start_ms=start_ms,
                    end_ms=None,
                    status=status,
                    start_line_offset=start_line_offset,
                )
            )

    return queries


def filter_queries(
    queries: list[LoggedQuery],
    mode: str,
    window_start_ms: int,
    window_end_ms: int,
) -> list[LoggedQuery]:
    """Select queries from the scanned window for the given mode.

    The given list is already the ACTIVE set (every query overlaps the
    window), so ACTIVE is the fallthrough that returns it unchanged.
    STARTS narrows to queries that began inside the window; ENDS
    narrows to queries that finished inside it. Still-running queries
    (`end_ms is None`) can satisfy STARTS but never ENDS.
    """
    if mode == "STARTS":
        return [
            query
            for query in queries
            if window_start_ms <= query.start_ms <= window_end_ms
        ]
    if mode == "ENDS":
        return [
            query
            for query in queries
            if query.end_ms is not None
            and window_start_ms <= query.end_ms <= window_end_ms
        ]
    return queries


def display_duration_ms(query: LoggedQuery, log_end_ms: int) -> int:
    """Duration to show for a query row; DURATION_UNKNOWN for orphans."""
    if query.status == "orphaned":
        return DURATION_UNKNOWN
    return (query.end_ms or log_end_ms) - query.start_ms


def materialize_rows(
    queries: list[LoggedQuery], log_end_ms: int
) -> list[HistoricQueryRow]:
    """Build a text-empty HistoricQueryRow for each given query.

    Called for the capped visible slice only. `duration_ms` is
    measured against `log_end_ms` for still-running queries, against
    the recorded `end_ms` for completed ones, and reported as
    DURATION_UNKNOWN for crash orphans. The `qid`, `client_ip`, and
    SPARQL text are left empty; `load_query_details_for_rows` fills
    them.
    """
    return [
        HistoricQueryRow(
            qid="",
            start_line_offset=query.start_line_offset,
            started_at_ms=query.start_ms,
            duration_ms=display_duration_ms(query, log_end_ms),
            status=query.status,
            sparql="",
            client_ip="",
        )
        for query in queries
    ]


def window_metrics(
    selected: list[LoggedQuery], slow_threshold_ms: int, label: str
) -> MetricsCounts:
    """Tally metrics over the completed queries in the selected set.

    The completed queries are fed to `metrics_for_queries` through a
    generator, so no per-query list is retained. Running and orphaned
    queries (`end_ms is None`) carry no real status and are excluded,
    so the counts match the completed rows on screen. Labelled with
    the current window size.
    """
    completed = (
        CompletedQuery(
            start_ms=query.start_ms,
            end_ms=query.end_ms,
            duration_ms=query.end_ms - query.start_ms,
            status=query.status,
            start_line_offset=query.start_line_offset,
        )
        for query in selected
        if query.end_ms is not None
    )
    snapshot = metrics_for_queries(completed, slow_threshold_ms)
    return MetricsCounts(label=label, **snapshot._asdict())


def passes_filter(
    query: LoggedQuery,
    filters: FilterState,
    log_end_ms: int,
) -> bool:
    """Whether a query survives the status and duration filters.

    A status filter keeps only the listed statuses. A duration filter
    keeps only queries at or above the minimum, which drops running and
    orphaned queries since their duration is below any real threshold.
    Text filters (client IP, SPARQL) need the start line and are
    applied separately by `filter_by_text`.
    """
    if filters.statuses and query.status not in filters.statuses:
        return False
    if filters.min_duration_s is not None:
        duration_ms = display_duration_ms(query, log_end_ms)
        if duration_ms < filters.min_duration_s * 1000:
            return False
    return True


def filter_rows(
    queries: list[LoggedQuery],
    filters: FilterState,
    log_end_ms: int,
) -> list[LoggedQuery]:
    """Keep the queries passing status/duration; same list when neither set."""
    if not filters.statuses and filters.min_duration_s is None:
        return queries
    return [
        query for query in queries if passes_filter(query, filters, log_end_ms)
    ]


def filter_by_text(
    log_path: Path,
    queries: list[LoggedQuery],
    filters: FilterState,
) -> list[LoggedQuery]:
    """Keep queries whose start-line text passes the text filters.

    Both tests are case-insensitive substring checks. The client IP
    test reads only the first HEAD_BYTES of the line. The SPARQL test
    escapes the search text like the log itself and searches the raw
    line bytes, so no line is decoded; only non-ASCII search text
    falls back to decoding every line. Returns the same list when no
    text filter is set.
    """
    if not filters.has_text_filter():
        return queries
    ip_search = (filters.client_ip_substr or "").lower()
    # An ASCII term matches case-insensitively; a term with any
    # non-ASCII char matches exactly, since bytes.lower() only folds
    # ASCII.
    raw_search = None
    ignore_case = True
    if filters.sparql_substr is not None:
        ignore_case = filters.sparql_substr.isascii()
        escaped = json.dumps(filters.sparql_substr, ensure_ascii=False)[1:-1]
        raw_search = (
            escaped.lower().encode() if ignore_case else escaped.encode()
        )
    ordered = sorted(queries, key=lambda query: query.start_line_offset)
    kept = []
    with open_log_buffer(log_path) as buf:
        if buf is None:
            return queries
        for query in ordered:
            offset = query.start_line_offset
            if filters.client_ip_substr is not None:
                # Bounded so a short line cannot spill the next line's
                # fields into the test.
                head_end = buf.find(b"\n", offset, offset + HEAD_BYTES)
                if head_end == -1:
                    head_end = offset + HEAD_BYTES
                head = buf[offset:head_end]
                client_ip = slice_string_value(head, CLIENT_IP_KEY) or ""
                if ip_search not in client_ip.lower():
                    continue
            if filters.sparql_substr is not None:
                line_end = buf.find(b"\n", offset)
                if line_end == -1:
                    line_end = len(buf)
                if not line_query_contains(
                    buf, offset, line_end, raw_search, ignore_case
                ):
                    continue
            kept.append(query)
    return kept


def load_query_details(
    log_path: Path,
    offsets: list[int],
    query_details_cache: dict[int, tuple[str, str, str]],
) -> None:
    """Fill the details cache for any of the given start-line offsets.

    The `qid`, `client_ip`, and SPARQL text live on each query's start
    line, which `read_window` did not read. Only the start of the query
    is read, since the table truncates it; the SparqlPane reads the full
    text on demand. Offsets already cached are reused; the rest are read
    in one pass, opening the file only when something is missing. The
    cache is scoped to one window so a sort or mode change repaints from
    memory.
    """
    missing = [
        offset for offset in offsets if offset not in query_details_cache
    ]
    if missing:
        with open_log_buffer(log_path) as buf:
            if buf is None:
                return
            for offset in missing:
                query_details_cache[offset] = load_sparql_snippet_at(
                    buf, offset
                )


def load_query_details_for_rows(
    log_path: Path,
    rows: list[HistoricQueryRow],
    query_details_cache: dict[int, tuple[str, str, str]],
) -> list[HistoricQueryRow]:
    """Fill the deferred start-line fields on the given rows.

    Reads each row's start line through `load_query_details`, reusing
    the cache, and returns filled copies; the input rows are left
    unchanged.
    """
    load_query_details(
        log_path,
        [row.start_line_offset for row in rows],
        query_details_cache,
    )
    filled_rows = []
    for row in rows:
        qid, client_ip, sparql = query_details_cache[row.start_line_offset]
        filled_rows.append(
            replace(row, qid=qid, client_ip=client_ip, sparql=sparql)
        )
    return filled_rows
