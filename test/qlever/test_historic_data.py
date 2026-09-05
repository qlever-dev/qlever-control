"""Tests for read_window, filtering, metrics, and detail loading."""

from qlever.monitor_queries.historic_data import (
    DURATION_UNKNOWN,
    LoggedQuery,
    filter_by_text,
    filter_queries,
    filter_rows,
    load_query_details_for_rows,
    materialize_rows,
    passes_filter,
    read_window,
    window_metrics,
)
from qlever.monitor_queries.models import FilterState, HistoricQueryRow

PAD_MS = 10_000
SLOW_MS = 60_000


def start_line(ts_ms, qid, query="SELECT 1", client_ip="x"):
    return (
        f'{{"ts-ms":{ts_ms},"event":"start","qid":"{qid}",'
        f'"client-ip":"{client_ip}","query":"{query}"}}\n'
    ).encode()


def end_line(ts_ms, qid, status="ok"):
    return (
        f'{{"ts-ms":{ts_ms},"event":"end","qid":"{qid}",'
        f'"status":"{status}"}}\n'
    ).encode()


def test_read_window_empty_log_returns_no_queries(write_log):
    path = write_log(b"")
    queries = read_window(path, 0, 1000, PAD_MS, log_end_ms=0, now_ms=0)
    assert queries == []


def test_read_window_keeps_pair_entirely_inside_window(write_log):
    path = write_log(
        start_line(1_000_000, "q1", "SELECT a") + end_line(1_000_500, "q1")
    )
    queries = read_window(
        path,
        900_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_000_500,
        now_ms=1_000_500,
    )
    assert queries == [
        LoggedQuery(
            start_ms=1_000_000,
            end_ms=1_000_500,
            status="ok",
            start_line_offset=0,
        )
    ]


def test_read_window_drops_pair_inside_pad_before_window(write_log):
    path = write_log(
        start_line(900_000, "pad", "SELECT pad")
        + end_line(900_500, "pad")
        + start_line(1_000_000, "q1", "SELECT a")
        + end_line(1_000_500, "q1")
    )
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_000_500,
        now_ms=1_000_500,
    )
    starts = [query.start_ms for query in queries]
    assert starts == [1_000_000]


def test_read_window_keeps_pair_straddling_window_start(write_log):
    path = write_log(
        start_line(995_000, "edge", "SELECT edge")
        + end_line(1_005_000, "edge")
    )
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_005_000,
        now_ms=1_005_000,
    )
    assert len(queries) == 1
    assert queries[0].start_ms == 995_000
    assert queries[0].end_ms == 1_005_000


def test_read_window_keeps_pair_straddling_window_end(write_log):
    path = write_log(
        start_line(1_095_000, "edge", "SELECT edge")
        + end_line(1_105_000, "edge")
    )
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_105_000,
        now_ms=1_105_000,
    )
    assert len(queries) == 1
    assert queries[0].start_ms == 1_095_000


def test_read_window_includes_still_open_with_running_status(write_log):
    path = write_log(start_line(1_050_000, "live", "SELECT live"))
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_050_000,
        now_ms=1_050_000,
    )
    assert queries == [
        LoggedQuery(
            start_ms=1_050_000,
            end_ms=None,
            status="running",
            start_line_offset=0,
        )
    ]


def test_read_window_marks_still_open_as_orphaned_when_log_advanced_past_pad(
    write_log,
):
    path = write_log(start_line(1_050_000, "ghost", "SELECT ghost"))
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_200_000,
        now_ms=1_200_000,
    )
    assert queries == [
        LoggedQuery(
            start_ms=1_050_000,
            end_ms=None,
            status="orphaned",
            start_line_offset=0,
        )
    ]


def test_read_window_marks_still_open_as_orphaned_when_log_is_stale(write_log):
    """A still-open query is orphaned once the log has been silent past 2t.

    The start is within pad of log_end_ms (gate (a) passes), but wall
    clock has moved past log_end_ms by more than pad (gate (b) fails),
    so the server must be dead and the survivor cannot be running.
    """
    path = write_log(start_line(1_050_000, "ghost", "SELECT ghost"))
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_050_000,
        now_ms=1_050_000 + PAD_MS + 1,
    )
    [query] = queries
    assert query.status == "orphaned"


def test_read_window_drops_still_open_started_after_window(write_log):
    path = write_log(start_line(1_200_000, "later", "SELECT later"))
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_200_000,
        now_ms=1_200_000,
    )
    assert queries == []


def test_read_window_keeps_all_overlapping_pairs(write_log):
    path = write_log(
        start_line(995_000, "edge_start", "SELECT a")
        + end_line(1_005_000, "edge_start")
        + start_line(1_050_000, "inside", "SELECT b")
        + end_line(1_060_000, "inside")
        + start_line(1_095_000, "edge_end", "SELECT c")
        + end_line(1_105_000, "edge_end")
    )
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_105_000,
        now_ms=1_105_000,
    )
    assert {query.start_ms for query in queries} == {
        995_000,
        1_050_000,
        1_095_000,
    }


def test_read_window_carries_real_start_line_offset(write_log):
    first = start_line(1_010_000, "q1", "SELECT a") + end_line(1_020_000, "q1")
    path = write_log(
        first
        + start_line(1_050_000, "q2", "SELECT b")
        + end_line(1_060_000, "q2")
    )
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_060_000,
        now_ms=1_060_000,
    )
    offsets = {query.start_ms: query.start_line_offset for query in queries}
    assert offsets[1_010_000] == 0
    assert offsets[1_050_000] == len(first)


def make_query(start_ms, end_ms, status="ok", start_line_offset=0):
    return LoggedQuery(
        start_ms=start_ms,
        end_ms=end_ms,
        status=status,
        start_line_offset=start_line_offset,
    )


def test_filter_queries_active_returns_list_unchanged():
    queries = [
        make_query(100, 200),
        make_query(300, None, status="running"),
    ]
    assert filter_queries(queries, "ACTIVE", 0, 1000) is queries


def test_filter_queries_starts_keeps_only_starts_in_window():
    queries = [
        make_query(50, 150),
        make_query(200, 400),
        make_query(1100, 1200),
    ]
    starts = [
        query.start_ms
        for query in filter_queries(queries, "STARTS", 100, 1000)
    ]
    assert starts == [200]


def test_filter_queries_starts_includes_running_queries():
    queries = [make_query(500, None, status="running")]
    starts = [
        query.start_ms
        for query in filter_queries(queries, "STARTS", 100, 1000)
    ]
    assert starts == [500]


def test_filter_queries_ends_excludes_running_queries():
    queries = [
        make_query(200, 500),
        make_query(300, None, status="running"),
    ]
    ends = [
        query.end_ms for query in filter_queries(queries, "ENDS", 100, 1000)
    ]
    assert ends == [500]


def test_filter_queries_ends_drops_ends_outside_window():
    queries = [
        make_query(50, 90),
        make_query(200, 500),
        make_query(800, 1200),
    ]
    ends = [
        query.end_ms for query in filter_queries(queries, "ENDS", 100, 1000)
    ]
    assert ends == [500]


def test_materialize_uses_recorded_end_for_completed_query():
    [row] = materialize_rows([make_query(1_100_000, 1_150_000)], 2_500_000)
    assert row.duration_ms == 50_000
    assert row.status == "ok"


def test_materialize_uses_log_end_for_running_query():
    [row] = materialize_rows(
        [make_query(1_100_000, None, status="running")], 2_500_000
    )
    assert row.duration_ms == 1_400_000
    assert row.status == "running"


def test_materialize_marks_orphan_duration_as_unknown():
    [row] = materialize_rows(
        [make_query(1_100_000, None, status="orphaned")], 2_500_000
    )
    assert row.duration_ms == DURATION_UNKNOWN
    assert row.status == "orphaned"


def test_materialize_carries_start_line_offset_onto_row():
    [row] = materialize_rows(
        [make_query(1_100_000, 1_200_000, start_line_offset=512)], 2_500_000
    )
    assert row.start_line_offset == 512


def test_materialize_leaves_text_empty():
    [row] = materialize_rows([make_query(1_100_000, 1_200_000)], 2_500_000)
    assert (row.qid, row.sparql, row.client_ip) == ("", "", "")


def test_materialize_after_mode_filter_keeps_only_mode_subset():
    queries = [
        make_query(1_100_000, 1_200_000),
        make_query(1_500_000, None, status="running"),
    ]
    selected = filter_queries(queries, "ENDS", 1_000_000, 2_000_000)
    rows = materialize_rows(selected, 2_500_000)
    assert [row.started_at_ms for row in rows] == [1_100_000]


def test_window_metrics_passes_window_size_as_label():
    queries = [make_query(100 + index, 200 + index) for index in range(7)]
    metrics = window_metrics(queries, SLOW_MS, "1h")
    assert metrics.label == "1h"
    assert metrics.seen == 7
    assert metrics.ok == 7


def test_window_metrics_exclude_running_and_orphaned():
    queries = [
        make_query(1_100_000, 1_900_000),
        make_query(1_500_000, None, status="running"),
        make_query(1_200_000, None, status="orphaned"),
    ]
    metrics = window_metrics(queries, SLOW_MS, "15m")
    assert metrics.seen == 1


# A is fully inside the window, B starts inside but ends after it, C
# starts before but ends inside, so the metric count changes with mode.
WINDOW_START_MS = 1_000_000
WINDOW_END_MS = 2_000_000


def metrics_for_mode(mode):
    """Metrics over the mode's subset of a fixed three-query window."""
    queries = [
        make_query(1_100_000, 1_900_000),
        make_query(1_500_000, 2_200_000),
        make_query(800_000, 1_300_000),
    ]
    selected = filter_queries(queries, mode, WINDOW_START_MS, WINDOW_END_MS)
    return window_metrics(selected, SLOW_MS, mode)


def test_metrics_active_counts_all_overlapping():
    assert metrics_for_mode("ACTIVE").seen == 3


def test_metrics_starts_counts_started_in_window():
    assert metrics_for_mode("STARTS").seen == 2


def test_metrics_ends_counts_ended_in_window():
    assert metrics_for_mode("ENDS").seen == 2


def make_row(start_line_offset):
    return HistoricQueryRow(
        qid="",
        start_line_offset=start_line_offset,
        started_at_ms=1_000_000,
        duration_ms=0,
        status="ok",
        sparql="",
        client_ip="",
    )


def test_load_query_details_fills_text_from_offsets(write_log):
    first = start_line(1_000_000, "q1", "SELECT a")
    path = write_log(first + start_line(1_050_000, "q2", "SELECT b"))
    cache = {}
    rows = [make_row(0), make_row(len(first))]
    filled = load_query_details_for_rows(path, rows, cache)
    assert [(row.qid, row.sparql) for row in filled] == [
        ("q1", "SELECT a"),
        ("q2", "SELECT b"),
    ]
    assert filled[0].client_ip == "x"
    assert cache[0] == ("q1", "x", "SELECT a")


def test_load_query_details_reuses_cache_without_reading(write_log):
    path = write_log(start_line(1_000_000, "q1", "SELECT a"))
    cache = {0: ("cached-qid", "cached-ip", "CACHED")}
    [filled] = load_query_details_for_rows(path, [make_row(0)], cache)
    assert filled.qid == "cached-qid"
    assert filled.client_ip == "cached-ip"
    assert filled.sparql == "CACHED"


FILTER_LOG_END_MS = 2_000_000


def filter_query(status="ok", duration_ms=5_000, start_line_offset=0):
    """Completed LoggedQuery whose display_duration_ms equals duration_ms."""
    start_ms = 1_000_000
    return LoggedQuery(
        start_ms=start_ms,
        end_ms=start_ms + duration_ms,
        status=status,
        start_line_offset=start_line_offset,
    )


def passes(query, filters):
    """Run passes_filter against the fixed log end."""
    return passes_filter(query, filters, FILTER_LOG_END_MS)


def test_passes_filter_empty_keeps_every_row():
    assert passes(filter_query(status="failed"), FilterState())


def test_passes_filter_status_keeps_listed_status():
    filters = FilterState(statuses=frozenset({"ok", "failed"}))
    assert passes(filter_query(status="ok"), filters)
    assert passes(filter_query(status="failed"), filters)


def test_passes_filter_status_drops_unlisted_status():
    filters = FilterState(statuses=frozenset({"ok"}))
    assert not passes(filter_query(status="timeout"), filters)


def test_passes_filter_duration_keeps_at_or_above_minimum():
    filters = FilterState(min_duration_s=5)
    assert passes(filter_query(duration_ms=5_000), filters)
    assert passes(filter_query(duration_ms=9_000), filters)


def test_passes_filter_duration_drops_below_minimum():
    filters = FilterState(min_duration_s=5)
    assert not passes(filter_query(duration_ms=4_999), filters)


def test_passes_filter_duration_drops_orphan_rows():
    filters = FilterState(min_duration_s=1)
    orphan = LoggedQuery(
        start_ms=1_000_000,
        end_ms=None,
        status="orphaned",
        start_line_offset=0,
    )
    assert not passes(orphan, filters)


def test_passes_filter_combines_status_and_duration():
    filters = FilterState(statuses=frozenset({"ok"}), min_duration_s=5)
    assert passes(filter_query(status="ok", duration_ms=6_000), filters)
    assert not passes(filter_query(status="ok", duration_ms=1_000), filters)
    assert not passes(
        filter_query(status="failed", duration_ms=6_000), filters
    )


def test_filter_rows_empty_returns_same_list():
    queries = [filter_query(status="ok"), filter_query(status="failed")]
    result = filter_rows(queries, FilterState(), FILTER_LOG_END_MS)
    assert result is queries


def test_filter_rows_text_only_filter_returns_same_list():
    queries = [filter_query(status="ok"), filter_query(status="failed")]
    filters = FilterState(sparql_substr="select")
    result = filter_rows(queries, filters, FILTER_LOG_END_MS)
    assert result is queries


def test_filter_rows_keeps_only_matching():
    queries = [
        filter_query(status="ok", duration_ms=6_000),
        filter_query(status="failed", duration_ms=6_000),
        filter_query(status="ok", duration_ms=1_000),
    ]
    filters = FilterState(statuses=frozenset({"ok"}), min_duration_s=5)
    assert filter_rows(queries, filters, FILTER_LOG_END_MS) == [queries[0]]


def text_log(write_log, entries):
    """Write a start line per (qid, query, client_ip) entry.

    Returns the log path and a LoggedQuery per entry pointing at its
    start-line offset, so filter_by_text can read each line's text.
    """
    payload = b""
    queries = []
    ts_ms = 1_000_000
    for qid, query, client_ip in entries:
        offset = len(payload)
        payload += start_line(ts_ms, qid, query, client_ip)
        queries.append(
            LoggedQuery(
                start_ms=ts_ms,
                end_ms=ts_ms + 1_000,
                status="ok",
                start_line_offset=offset,
            )
        )
        ts_ms += 1_000
    return write_log(payload), queries


def test_filter_by_text_no_filter_returns_same_list(write_log):
    path, queries = text_log(write_log, [("q1", "SELECT a", "x")])
    assert filter_by_text(path, queries, FilterState()) is queries


def test_filter_by_text_client_ip_matches_substring_ignoring_case(write_log):
    path, queries = text_log(
        write_log,
        [("q1", "SELECT a", "192.168.0.7"), ("q2", "SELECT b", "10.0.0.1")],
    )
    filters = FilterState(client_ip_substr="192.168")
    assert filter_by_text(path, queries, filters) == [queries[0]]


def test_filter_by_text_client_ip_is_case_insensitive(write_log):
    path, queries = text_log(write_log, [("q1", "SELECT a", "fe80::ABCD")])
    filters = FilterState(client_ip_substr="abcd")
    assert filter_by_text(path, queries, filters) == [queries[0]]


def test_filter_by_text_sparql_matches_substring_ignoring_case(write_log):
    path, queries = text_log(
        write_log, [("q1", "SELECT a", "x"), ("q2", "ASK b", "x")]
    )
    filters = FilterState(sparql_substr="select")
    assert filter_by_text(path, queries, filters) == [queries[0]]


def test_filter_by_text_combines_client_ip_and_sparql(write_log):
    path, queries = text_log(
        write_log,
        [
            ("q1", "SELECT a", "192.168.0.7"),
            ("q2", "SELECT b", "10.0.0.1"),
            ("q3", "ASK c", "192.168.0.9"),
        ],
    )
    filters = FilterState(client_ip_substr="192.168", sparql_substr="select")
    assert filter_by_text(path, queries, filters) == [queries[0]]


def test_filter_by_text_sparql_with_quotes_matches_escaped_line(write_log):
    # The file holds \"Berlin\"; the filter uses plain quotes.
    path, queries = text_log(
        write_log,
        [("q1", 'name \\"Berlin\\"', "x"), ("q2", "SELECT b", "x")],
    )
    filters = FilterState(sparql_substr='"berlin"')
    assert filter_by_text(path, queries, filters) == [queries[0]]


def test_filter_by_text_sparql_with_newline_matches_escaped_line(write_log):
    # The file holds line1\nline2; the filter uses a real newline.
    path, queries = text_log(
        write_log,
        [("q1", "line1\\nline2", "x"), ("q2", "SELECT b", "x")],
    )
    filters = FilterState(sparql_substr="e1\nl")
    assert filter_by_text(path, queries, filters) == [queries[0]]


def test_filter_by_text_rejects_fake_byte_hit(write_log):
    # The query is a, backslash, n, t, b, stored as a\\ntb. Those bytes
    # contain \nt, the escaped form of newline + t, but the decoded
    # query has no newline, so the query must be dropped.
    path, queries = text_log(write_log, [("q1", "a\\\\ntb", "x")])
    filters = FilterState(sparql_substr="\nt")
    assert filter_by_text(path, queries, filters) == []


def test_filter_by_text_non_ascii_search_text_matches(write_log):
    path, queries = text_log(
        write_log,
        [
            ("q1", "SELECT ?s { ?s rdfs:label 'Zürich' }", "x"),
            ("q2", "SELECT b", "x"),
        ],
    )
    # A non-ASCII term matches exactly, so the same case is kept.
    filters = FilterState(sparql_substr="Zürich")
    assert filter_by_text(path, queries, filters) == [queries[0]]


def test_filter_by_text_non_ascii_search_text_is_case_sensitive(write_log):
    path, queries = text_log(
        write_log, [("q1", "SELECT ?s { ?s rdfs:label 'Zürich' }", "x")]
    )
    # Different case on the non-ASCII term does not match.
    filters = FilterState(sparql_substr="zürich")
    assert filter_by_text(path, queries, filters) == []


def test_filter_by_text_client_ip_on_line_longer_than_head(write_log):
    long_query = "SELECT " + "x" * 1000
    path, queries = text_log(
        write_log,
        [("q1", long_query, "192.168.0.7"), ("q2", long_query, "10.0.0.1")],
    )
    filters = FilterState(client_ip_substr="192.168")
    assert filter_by_text(path, queries, filters) == [queries[0]]
