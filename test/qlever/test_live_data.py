"""Tests for the Live data layer: state, tailer, boot, metrics."""

import threading
import time

from qlever.monitor_queries.live_data import (
    LIVE_HORIZON_MS,
    ActiveQuery,
    CompletedQueries,
    LiveLogReader,
    LiveState,
    discard_finished_backlog,
    find_active_queries,
    get_live_metrics,
    get_live_query_rows,
    load_completed_history,
)
from qlever.monitor_queries.log_reader import CompletedQuery
from qlever.monitor_queries.metrics import MetricsSnapshot, percentiles

NOW_MS = 1_700_000_000_000
MIN_MS = 60_000
WINDOW_5M = 5 * MIN_MS
WINDOW_15M = 15 * MIN_MS


def make_completed(end_ms, status="ok", duration_ms=100):
    """Build a CompletedQuery with a self-consistent start_ms."""
    return CompletedQuery(
        start_ms=end_ms - duration_ms,
        end_ms=end_ms,
        duration_ms=duration_ms,
        status=status,
        start_line_offset=0,
    )


def test_empty_history_returns_empty_snapshot():
    history = CompletedQueries()
    [snap] = history.metrics_for_windows(
        now_ms=NOW_MS,
        windows_ms=[WINDOW_5M],
        slow_threshold_ms=10_000,
        data_start_ms=NOW_MS - WINDOW_5M,
    )
    assert snap == MetricsSnapshot(
        seen=0,
        ok=0,
        failed=0,
        timeout=0,
        cancelled=0,
        unknown=0,
        slow=0,
        am=None,
        gm=None,
        p50=None,
        p95=None,
    )


def test_status_counts_split_correctly():
    history = CompletedQueries()
    for status in ("ok", "failed", "timeout", "cancelled", "unknown"):
        history.add(make_completed(NOW_MS - MIN_MS, status=status))
    [snap] = history.metrics_for_windows(
        now_ms=NOW_MS,
        windows_ms=[WINDOW_5M],
        slow_threshold_ms=10_000,
        data_start_ms=NOW_MS - WINDOW_5M,
    )
    assert snap.seen == 5
    assert snap.ok == 1
    assert snap.failed == 1
    assert snap.timeout == 1
    assert snap.cancelled == 1
    assert snap.unknown == 1


def test_window_excludes_old_entries():
    history = CompletedQueries()
    history.add(make_completed(NOW_MS - 2 * MIN_MS))
    history.add(make_completed(NOW_MS - WINDOW_5M))
    history.add(make_completed(NOW_MS - 10 * MIN_MS))
    [snap] = history.metrics_for_windows(
        now_ms=NOW_MS,
        windows_ms=[WINDOW_5M],
        slow_threshold_ms=10_000,
        data_start_ms=NOW_MS - WINDOW_5M,
    )
    assert snap.seen == 2


def test_partial_coverage_returns_none():
    history = CompletedQueries()
    history.add(make_completed(NOW_MS - 2 * MIN_MS))
    snaps = history.metrics_for_windows(
        now_ms=NOW_MS,
        windows_ms=[WINDOW_5M, WINDOW_15M],
        slow_threshold_ms=10_000,
        data_start_ms=NOW_MS - 10 * MIN_MS,
    )
    assert snaps[0] is not None
    assert snaps[0].seen == 1
    assert snaps[1] is None


def test_slow_threshold_independent_of_status():
    history = CompletedQueries()
    history.add(
        make_completed(NOW_MS - MIN_MS, status="ok", duration_ms=12_000)
    )
    [snap] = history.metrics_for_windows(
        now_ms=NOW_MS,
        windows_ms=[WINDOW_5M],
        slow_threshold_ms=10_000,
        data_start_ms=NOW_MS - WINDOW_5M,
    )
    assert snap.ok == 1
    assert snap.slow == 1


def test_drop_older_than_pops_left():
    history = CompletedQueries()
    history.add(make_completed(NOW_MS - 30 * MIN_MS))
    history.add(make_completed(NOW_MS - 20 * MIN_MS))
    history.add(make_completed(NOW_MS - MIN_MS))
    history.drop_older_than(NOW_MS - 15 * MIN_MS)
    assert len(history.entries) == 1
    assert history.entries[0].end_ms == NOW_MS - MIN_MS


def test_percentiles_with_known_values():
    durations = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
    p50, p95 = percentiles(durations)
    assert 450 <= p50 <= 600
    assert 900 <= p95 <= 1100


def test_data_start_equal_to_cutoff_returns_snapshot():
    history = CompletedQueries()
    history.add(make_completed(NOW_MS - 2 * MIN_MS))
    cutoff = NOW_MS - WINDOW_5M
    [snap] = history.metrics_for_windows(
        now_ms=NOW_MS,
        windows_ms=[WINDOW_5M],
        slow_threshold_ms=10_000,
        data_start_ms=cutoff,
    )
    assert snap is not None
    assert snap.seen == 1


def start_line(ts_ms, qid, query="SELECT 1"):
    return (
        f'{{"ts-ms":{ts_ms},"event":"start","qid":"{qid}",'
        f'"client-ip":"x","query":"{query}"}}\n'
    ).encode()


def end_line(ts_ms, qid, status="ok"):
    return (
        f'{{"ts-ms":{ts_ms},"event":"end","qid":"{qid}",'
        f'"status":"{status}"}}\n'
    ).encode()


def test_find_active_queries_picks_up_unmatched_starts(write_log):
    path = write_log(
        start_line(1000, "q1", "SELECT a")
        + start_line(2000, "q2", "SELECT b")
        + end_line(3000, "q1")
    )
    state, cut_offset, eof_ts = find_active_queries(path, window_pad_ms=10_000)
    assert list(state.active) == ["q2"]
    assert state.active["q2"] == ActiveQuery(
        start_ms=2000, end_ms=None, client_ip="x", sparql="SELECT b"
    )
    assert cut_offset == path.stat().st_size
    assert eof_ts == 3000


def test_find_active_queries_loads_sparql_for_each_survivor(write_log):
    path = write_log(
        start_line(1000, "a", "SELECT alpha")
        + start_line(2000, "b", "SELECT beta")
    )
    state, _, _ = find_active_queries(path, window_pad_ms=10_000)
    assert state.active["a"] == ActiveQuery(
        start_ms=1000, end_ms=None, client_ip="x", sparql="SELECT alpha"
    )
    assert state.active["b"] == ActiveQuery(
        start_ms=2000, end_ms=None, client_ip="x", sparql="SELECT beta"
    )


def test_find_active_queries_empty_log_returns_empty_state(write_log):
    path = write_log(b"")
    state, cut_offset, eof_ts = find_active_queries(path, window_pad_ms=10_000)
    assert state.active == {}
    assert cut_offset == 0
    assert eof_ts == 0


def test_find_active_queries_seeds_latest_event_ms_from_eof(write_log):
    path = write_log(
        start_line(1000, "q1") + end_line(2000, "q1") + start_line(3000, "q2")
    )
    state, _, eof_ts = find_active_queries(path, window_pad_ms=10_000)
    assert state.latest_event_ms == eof_ts == 3000


def test_find_active_queries_empty_log_leaves_latest_event_ms_none(write_log):
    path = write_log(b"")
    state, _, _ = find_active_queries(path, window_pad_ms=10_000)
    assert state.latest_event_ms is None


def make_reader(path, state, cut_offset=0, window_pad_ms=10_000, now_ms=3000):
    """Build a LiveLogReader with a frozen clock for poll tests."""
    return LiveLogReader(
        log_path=path,
        state=state,
        cut_offset=cut_offset,
        window_pad_ms=window_pad_ms,
        now_ms=lambda: now_ms,
    )


def test_poll_pairs_a_clean_start_and_end(write_log):
    path = write_log(start_line(1000, "q1", "SELECT a") + end_line(2000, "q1"))
    state = LiveState()
    reader = make_reader(path, state)
    with path.open("rb") as log_stream:
        reader.poll(log_stream)
    # The end event records the completion but the query stays in active
    # (end_ms set, still unseen) until a repaint has shown it.
    entry = state.active["q1"]
    assert entry.end_ms == 2000
    assert entry.seen is False
    [done] = state.completed.entries
    assert done.start_ms == 1000
    assert done.end_ms == 2000
    assert done.duration_ms == 1000
    assert done.status == "ok"
    assert done.start_line_offset is None


def test_poll_keeps_start_in_active_until_end_arrives(write_log):
    path = write_log(start_line(1000, "q1", "SELECT a"))
    state = LiveState()
    reader = make_reader(path, state)
    with path.open("rb") as log_stream:
        reader.poll(log_stream)
    assert state.active == {
        "q1": ActiveQuery(
            start_ms=1000, end_ms=None, client_ip="x", sparql="SELECT a"
        )
    }
    assert len(state.completed.entries) == 0


def test_poll_advances_cursor_so_second_poll_sees_new_lines(write_log):
    first = start_line(1000, "q1") + end_line(2000, "q1")
    path = write_log(first)
    state = LiveState()
    reader = make_reader(path, state)
    with path.open("rb") as log_stream:
        reader.poll(log_stream)
    assert len(state.completed.entries) == 1

    path.write_bytes(
        first + start_line(3000, "q2") + end_line(4000, "q2", "failed")
    )
    with path.open("rb") as log_stream:
        reader.poll(log_stream)
    assert len(state.completed.entries) == 2
    assert state.completed.entries[-1].status == "failed"


def test_poll_partial_trailing_line_is_left_for_next_poll(write_log):
    full = start_line(1000, "q1") + end_line(2000, "q1")
    truncated = full[:-5]
    path = write_log(truncated)
    state = LiveState()
    reader = make_reader(path, state)
    with path.open("rb") as log_stream:
        reader.poll(log_stream)
    assert "q1" in state.active
    assert len(state.completed.entries) == 0

    path.write_bytes(full)
    with path.open("rb") as log_stream:
        reader.poll(log_stream)
    assert state.active["q1"].end_ms == 2000
    assert len(state.completed.entries) == 1


def test_poll_skips_malformed_line_and_continues(write_log):
    path = write_log(
        b"not json at all\n" + start_line(1000, "q1") + end_line(2000, "q1")
    )
    state = LiveState()
    reader = make_reader(path, state)
    with path.open("rb") as log_stream:
        reader.poll(log_stream)
    assert len(state.completed.entries) == 1


def test_poll_drops_end_without_matching_start(write_log):
    path = write_log(end_line(2000, "ghost"))
    state = LiveState()
    reader = make_reader(path, state)
    with path.open("rb") as log_stream:
        reader.poll(log_stream)
    assert state.active == {}
    assert len(state.completed.entries) == 0


def test_poll_advances_latest_event_ms_to_the_newest_line(write_log):
    path = write_log(start_line(1000, "q1") + end_line(2500, "q1"))
    state = LiveState()
    reader = make_reader(path, state)
    with path.open("rb") as log_stream:
        reader.poll(log_stream)
    assert state.latest_event_ms == 2500


def test_poll_keeps_latest_event_ms_monotonic_under_out_of_order_writes(
    write_log,
):
    # Sub-millisecond jitter on the server can write an older ts_ms
    # after a newer one; the field must stay at the larger value.
    path = write_log(start_line(2000, "q_late") + start_line(1500, "q_early"))
    state = LiveState()
    reader = make_reader(path, state)
    with path.open("rb") as log_stream:
        reader.poll(log_stream)
    assert state.latest_event_ms == 2000


def test_evict_stale_drops_actives_older_than_window_pad(write_log):
    state = LiveState()
    now = 1_000_000
    pad = 10_000
    state.active["fresh"] = ActiveQuery(
        start_ms=now - 1_000, end_ms=None, client_ip="", sparql=""
    )
    state.active["old"] = ActiveQuery(
        start_ms=now - 20_000, end_ms=None, client_ip="", sparql=""
    )
    reader = LiveLogReader(
        log_path=write_log(b""),
        state=state,
        cut_offset=0,
        window_pad_ms=pad,
        now_ms=lambda: now,
    )
    reader.evict_stale()
    assert list(state.active) == ["fresh"]


def test_evict_stale_drops_completed_older_than_one_hour(write_log):
    state = LiveState()
    now = 10_000_000
    state.completed.add(
        CompletedQuery(
            start_ms=now - LIVE_HORIZON_MS - 1_000,
            end_ms=now - LIVE_HORIZON_MS - 500,
            duration_ms=500,
            status="ok",
            start_line_offset=None,
        )
    )
    state.completed.add(
        CompletedQuery(
            start_ms=now - 1_000,
            end_ms=now - 500,
            duration_ms=500,
            status="ok",
            start_line_offset=None,
        )
    )
    reader = LiveLogReader(
        log_path=write_log(b""),
        state=state,
        cut_offset=0,
        window_pad_ms=10_000,
        now_ms=lambda: now,
    )
    reader.evict_stale()
    assert len(state.completed.entries) == 1
    assert state.completed.entries[0].end_ms == now - 500


def test_get_live_metrics_returns_three_rows_when_coverage_spans_an_hour():
    state = LiveState()
    state.metrics_known_from_ms = NOW_MS - 65 * MIN_MS
    state.completed.add(make_completed(NOW_MS - 30 * MIN_MS))
    state.completed.add(make_completed(NOW_MS - 3 * MIN_MS))
    state.completed.add(make_completed(NOW_MS - MIN_MS // 2))

    rows = get_live_metrics(state, slow_threshold_ms=10_000, now_ms=NOW_MS)
    assert [row.label for row in rows] == ["last 1m", "last 5m", "last 1h"]
    row_1m, row_5m, row_1h = rows
    assert row_1m.seen == 1
    assert row_5m.seen == 2
    assert row_1h.seen == 3


def test_get_live_metrics_blanks_windows_past_coverage_start():
    state = LiveState()
    state.metrics_known_from_ms = NOW_MS - 3 * MIN_MS
    state.completed.add(make_completed(NOW_MS - MIN_MS // 2))

    row_1m, row_5m, row_1h = get_live_metrics(
        state,
        slow_threshold_ms=10_000,
        now_ms=NOW_MS,
    )
    assert row_1m.seen == 1
    assert row_1m.not_ready_message is None
    assert row_5m.seen is None
    assert row_5m.not_ready_message == "ready in 2m"
    assert row_1h.seen is None
    assert row_1h.not_ready_message == "ready in 57m"


def test_get_live_metrics_masks_everything_before_coverage_is_announced():
    state = LiveState()
    state.completed.add(make_completed(NOW_MS - 30 * MIN_MS))

    row_5m, row_15m, row_1h = get_live_metrics(
        state,
        slow_threshold_ms=10_000,
        now_ms=NOW_MS,
    )
    # During boot ramp we don't know when coverage arrives, so no ETA.
    for row in (row_5m, row_15m, row_1h):
        assert row.seen is None
        assert row.not_ready_message is None


def test_load_completed_history_loads_pairs_from_history(write_log):
    path = write_log(
        start_line(1000, "q1")
        + end_line(2000, "q1")
        + start_line(3000, "q2")
        + end_line(4000, "q2", "failed")
    )
    state = LiveState()
    load_completed_history(
        log_path=path,
        state=state,
        cut_offset=path.stat().st_size,
        window_pad_ms=10_000,
        now_ms=lambda: 5000,
    )
    assert len(state.completed.entries) == 2
    assert state.completed.entries[0].start_ms == 1000
    assert state.completed.entries[0].end_ms == 2000
    assert state.completed.entries[1].status == "failed"


def test_load_completed_history_prepends_before_existing_tailer_entries(
    write_log,
):
    path = write_log(start_line(1000, "old") + end_line(2000, "old"))
    state = LiveState()
    state.completed.add(
        CompletedQuery(
            start_ms=5000,
            end_ms=6000,
            duration_ms=1000,
            status="ok",
            start_line_offset=None,
        )
    )
    load_completed_history(
        log_path=path,
        state=state,
        cut_offset=path.stat().st_size,
        window_pad_ms=10_000,
        now_ms=lambda: 7000,
    )
    assert len(state.completed.entries) == 2
    assert state.completed.entries[0].end_ms == 2000
    assert state.completed.entries[1].end_ms == 6000


def test_load_completed_history_drops_starts_without_a_matching_end(write_log):
    path = write_log(
        start_line(1000, "q1")
        + end_line(2000, "q1")
        + start_line(3000, "orphan")
    )
    state = LiveState()
    load_completed_history(
        log_path=path,
        state=state,
        cut_offset=path.stat().st_size,
        window_pad_ms=10_000,
        now_ms=lambda: 5000,
    )
    assert len(state.completed.entries) == 1
    assert state.completed.entries[0].start_ms == 1000


def test_load_completed_history_recovers_query_straddling_the_hour(write_log):
    oldest_wanted = 100_000
    now = LIVE_HORIZON_MS + oldest_wanted
    path = write_log(
        start_line(91_000, "before")
        + end_line(93_000, "before")
        + start_line(95_000, "straddler")
        + end_line(150_000, "straddler")
        + start_line(200_000, "inside")
        + end_line(300_000, "inside")
    )
    state = LiveState()
    load_completed_history(
        log_path=path,
        state=state,
        cut_offset=path.stat().st_size,
        window_pad_ms=20_000,
        now_ms=lambda: now,
    )
    starts = [entry.start_ms for entry in state.completed.entries]
    assert starts == [95_000, 200_000]


def test_poll_loop_picks_up_lines_appended_to_the_file(write_log):
    path = write_log(b"")
    state = LiveState()
    reader = LiveLogReader(
        log_path=path,
        state=state,
        cut_offset=0,
        window_pad_ms=10_000,
        poll_interval=0.02,
        now_ms=lambda: 0,
    )
    stop_event = threading.Event()

    def run_loop():
        with path.open("rb") as stream:
            while not stop_event.is_set():
                reader.poll(stream)
                stop_event.wait(reader.poll_interval)

    thread = threading.Thread(target=run_loop, daemon=True)
    thread.start()
    try:
        with path.open("ab") as appender:
            appender.write(start_line(1000, "q1"))
            appender.write(end_line(2000, "q1"))
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if len(state.completed.entries) == 1:
                break
            time.sleep(0.02)
        assert len(state.completed.entries) == 1
    finally:
        stop_event.set()
        thread.join()


def test_finished_query_is_shown_once_then_skipped(write_log):
    """A query that finished before any paint shows once, then is hidden."""
    path = write_log(start_line(1000, "q1", "SELECT a") + end_line(1100, "q1"))
    state = LiveState()
    reader = make_reader(path, state)
    with path.open("rb") as log_stream:
        reader.poll(log_stream)
    [row] = get_live_query_rows(state, now_ms=5000)
    assert row.qid == "q1"
    assert row.started_at_ms == 1000
    assert row.sparql == "SELECT a"
    # Frozen at end - start, regardless of the clock the caller passes.
    assert row.duration_ms == 100
    assert get_live_query_rows(state, now_ms=9999) == []


def test_finished_unseen_entry_survives_eviction_until_shown(write_log):
    """Eviction keeps a finished query until a repaint has rendered it."""
    path = write_log(start_line(1000, "q1") + end_line(1100, "q1"))
    state = LiveState()
    reader = make_reader(path, state, now_ms=1200)
    with path.open("rb") as log_stream:
        reader.poll(log_stream)
    assert "q1" in state.active
    get_live_query_rows(state, now_ms=1200)
    reader.evict_stale()
    assert "q1" not in state.active


def test_discard_finished_backlog_marks_finished_seen_only():
    """The resume drop suppresses finished entries but keeps running ones."""
    state = LiveState()
    state.active["done"] = ActiveQuery(
        start_ms=1000, end_ms=1100, client_ip="x", sparql="A"
    )
    state.active["running"] = ActiveQuery(
        start_ms=1200, end_ms=None, client_ip="y", sparql="B"
    )
    discard_finished_backlog(state)
    assert state.active["done"].seen is True
    assert state.active["running"].seen is False
    shown = [row.qid for row in get_live_query_rows(state, now_ms=2000)]
    assert shown == ["running"]


def test_get_live_query_rows_uses_caller_clock_for_duration():
    """Duration is now_ms - start_ms; the screen passes its display clock."""
    state = LiveState()
    state.active["q1"] = ActiveQuery(
        start_ms=1000, end_ms=None, client_ip="x", sparql="SELECT a"
    )
    state.active["q2"] = ActiveQuery(
        start_ms=1500, end_ms=None, client_ip="y", sparql="SELECT b"
    )
    rows = {row.qid: row for row in get_live_query_rows(state, now_ms=2000)}
    assert rows["q1"].duration_ms == 1000
    assert rows["q2"].duration_ms == 500
    assert rows["q1"].started_at_ms == 1000
    assert rows["q2"].client_ip == "y"
