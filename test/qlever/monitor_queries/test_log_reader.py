import pytest

from qlever.monitor_queries import log_reader
from qlever.monitor_queries.log_reader import (
    SNIPPET_BYTES,
    CompletedQuery,
    extract_qid_ip_query,
    line_query_contains,
    load_sparql_at,
    load_sparql_snippet_at,
    next_whole_line,
    offset_for_ts,
    open_log_buffer,
    pair_start_end_events,
    parse_line,
    parse_line_fallback,
    peek_ts_ms,
    read_first_timestamp,
    read_last_timestamp,
    scan_range,
)

START = (
    b'{"ts-ms":1716000000000,"event":"start","qid":"q-8a4f",'
    b'"client-ip":"1.2.3.4","query":"SELECT * WHERE { ?s ?p ?o }"}'
)
END = b'{"ts-ms":1716000000050,"event":"end","qid":"q-8a4f","status":"ok"}'

VALID_STATUSES = ["ok", "failed", "cancelled", "timeout"]


def test_parse_line_start_has_no_status_and_ignores_query():
    assert parse_line(START) == (1716000000000, "start", "q-8a4f", None)


@pytest.mark.parametrize("status", VALID_STATUSES)
def test_parse_line_end_carries_each_valid_status(status):
    line = b'{"ts-ms":1,"event":"end","qid":"q1","status":"%s"}' % (
        status.encode()
    )
    assert parse_line(line) == (1, "end", "q1", status)


def test_parse_line_trailing_newline_tolerated():
    assert parse_line(END + b"\n") == (1716000000050, "end", "q-8a4f", "ok")


def test_parse_line_escaped_quote_in_query_does_not_break_qid():
    line = (
        b'{"ts-ms":1,"event":"start","qid":"q1",'
        b'"client-ip":"::1","query":"FILTER(?x = \\"a\\")"}'
    )
    assert parse_line(line) == (1, "start", "q1", None)


def test_parse_line_wrong_first_key_is_a_miss():
    assert parse_line(b'{"event":"end","ts-ms":1,"qid":"q1"}') is None


def test_parse_line_non_integer_ts_is_a_miss():
    assert parse_line(b'{"ts-ms":1.5,"event":"end","qid":"q1"}') is None


def test_parse_line_unknown_event_is_a_miss():
    assert parse_line(b'{"ts-ms":1,"event":"ping","qid":"q1"}') is None


def test_parse_line_end_status_outside_known_set_maps_to_unknown():
    line = b'{"ts-ms":1,"event":"end","qid":"q1","status":"weird"}'
    assert parse_line(line) == (1, "end", "q1", "unknown")


def test_parse_line_missing_qid_is_a_miss():
    assert parse_line(b'{"ts-ms":1,"event":"end","status":"ok"}') is None


def test_fallback_recovers_a_line_the_fast_path_rejects():
    # Leading space after `{` defeats the byte prefix but is valid
    # JSON, so the fallback still extracts the fields.
    line = b'{ "ts-ms":1716000000000,"event":"end","qid":"q1","status":"ok"}'
    assert parse_line(line) is None
    assert parse_line_fallback(line) == (1716000000000, "end", "q1", "ok")


def test_fallback_start_returns_none_status():
    assert parse_line_fallback(START) == (
        1716000000000,
        "start",
        "q-8a4f",
        None,
    )


def test_fallback_malformed_json_returns_none():
    assert parse_line_fallback(b'{"ts-ms":1,') is None


def test_fallback_non_object_json_returns_none():
    assert parse_line_fallback(b"[1,2,3]") is None


def test_fallback_non_integer_ts_returns_none():
    line = b'{"ts-ms":1.0,"event":"end","qid":"q1","status":"ok"}'
    assert parse_line_fallback(line) is None


def test_fallback_status_outside_known_set_maps_to_unknown():
    line = b'{"ts-ms":1,"event":"end","qid":"q1","status":"weird"}'
    assert parse_line_fallback(line) == (1, "end", "q1", "unknown")


FIRST_LINE = b'{"ts-ms":1000,"event":"start","qid":"q1","query":"SELECT 1"}\n'
SECOND_LINE = b'{"ts-ms":2000,"event":"end","qid":"q1","status":"ok"}\n'
THIRD_LINE = b'{"ts-ms":3000,"event":"start","qid":"q2","query":"SELECT 2"}\n'
SECOND_OFFSET = len(FIRST_LINE)
THIRD_OFFSET = len(FIRST_LINE) + len(SECOND_LINE)


def test_next_whole_line_probe_zero_returns_first_line():
    buf = FIRST_LINE + SECOND_LINE + THIRD_LINE
    assert next_whole_line(buf, 0) == (0, 1000)


def test_next_whole_line_mid_line_aligns_to_following_line():
    buf = FIRST_LINE + SECOND_LINE + THIRD_LINE
    # A probe inside the first line lands on the second.
    assert next_whole_line(buf, 10) == (SECOND_OFFSET, 2000)
    assert next_whole_line(buf, SECOND_OFFSET - 1) == (SECOND_OFFSET, 2000)


def test_next_whole_line_on_a_boundary_advances_to_the_next_line():
    buf = FIRST_LINE + SECOND_LINE + THIRD_LINE
    # probe > 0 always discards one line, so a probe on the second
    # line's start returns the third.
    assert next_whole_line(buf, SECOND_OFFSET) == (THIRD_OFFSET, 3000)


def test_next_whole_line_past_eof_returns_none():
    buf = FIRST_LINE + SECOND_LINE
    assert next_whole_line(buf, len(buf)) is None


def test_next_whole_line_trailing_partial_line_returns_none():
    partial = b'{"ts-ms":4000,"event":"end","qid":"q3"'
    buf = FIRST_LINE + partial
    # The only line after the probe has no newline, so it is incomplete.
    assert next_whole_line(buf, 5) is None


def build_log(timestamps):
    """Bytes for an ascending-ts log plus each line's start offset."""
    data = bytearray()
    offsets = []
    for i, ts in enumerate(timestamps):
        offsets.append(len(data))
        data += b'{"ts-ms":%d,"event":"end","qid":"q%d","status":"ok"}\n' % (
            ts,
            i,
        )
    return bytes(data), offsets


def first_ts_at_or_after(buf, offset, target):
    """ts of the first line at/after offset, or None if every line older."""
    while offset < len(buf):
        end = buf.find(b"\n", offset)
        if end == -1:
            return None
        ts = peek_ts_ms(buf[offset:end])
        if ts is not None and ts >= target:
            return ts
        offset = end + 1
    return None


def test_offset_for_ts_target_before_first_line_returns_zero():
    data, _ = build_log([1000, 1010, 1020])
    assert offset_for_ts(data, 500) == 0


def test_offset_for_ts_target_equal_to_first_line_returns_zero():
    data, _ = build_log([1000, 1010, 1020])
    # first_ts >= target means every line qualifies; start at 0.
    assert offset_for_ts(data, 1000) == 0


@pytest.mark.parametrize("target", [1015, 1020, 1099, 1500, 1900])
def test_offset_for_ts_lands_at_or_before_the_boundary(monkeypatch, target):
    # Tiny gallop step so the backward gallop actually iterates here.
    monkeypatch.setattr(log_reader, "GALLOP_START", 16)
    timestamps = list(range(1000, 2000, 10))
    data, offsets = build_log(timestamps)

    result = offset_for_ts(data, target)

    # Never overshoots: the returned line is not newer than target, so
    # no line with ts >= target was skipped.
    boundary = next(o for o, ts in zip(offsets, timestamps) if ts >= target)
    assert result <= boundary
    assert first_ts_at_or_after(data, result, target) == next(
        ts for ts in timestamps if ts >= target
    )


def test_offset_for_ts_target_past_last_line_yields_empty_scan(monkeypatch):
    monkeypatch.setattr(log_reader, "GALLOP_START", 16)
    data, _ = build_log(list(range(1000, 2000, 10)))

    result = offset_for_ts(data, 9999)

    # Near EOF, not necessarily len(buf), but a forward scan from it
    # finds nothing at or after the target: the same empty result.
    assert first_ts_at_or_after(data, result, 9999) is None


def test_scan_range_yields_each_whole_line_with_its_offset():
    data, offsets = build_log([1000, 1010, 1020])
    assert list(scan_range(data, 0, len(data))) == [
        ((1000, "end", "q0", "ok"), offsets[0]),
        ((1010, "end", "q1", "ok"), offsets[1]),
        ((1020, "end", "q2", "ok"), offsets[2]),
    ]


def test_scan_range_includes_the_line_straddling_hi_bound():
    data, offsets = build_log([1000, 1010, 1020])
    # hi_bound falls inside the second line. The second line starts
    # before it so it is still emitted; the third starts past it.
    result = list(scan_range(data, 0, offsets[1] + 3))
    assert [ts for (ts, _, _, _), _ in result] == [1000, 1010]


def test_scan_range_stops_before_a_trailing_partial_line():
    data, _ = build_log([1000])
    buf = data + b'{"ts-ms":2000,"event":"end","qid":"q9"'
    assert list(scan_range(buf, 0, len(buf))) == [
        ((1000, "end", "q0", "ok"), 0),
    ]


def test_scan_range_skips_a_malformed_line_and_continues():
    good = b'{"ts-ms":1000,"event":"end","qid":"q0","status":"ok"}\n'
    junk = b"not json at all\n"
    tail = b'{"ts-ms":1020,"event":"end","qid":"q2","status":"ok"}\n'
    buf = good + junk + tail
    assert list(scan_range(buf, 0, len(buf))) == [
        ((1000, "end", "q0", "ok"), 0),
        ((1020, "end", "q2", "ok"), len(good) + len(junk)),
    ]


def test_scan_range_recovers_a_line_via_the_json_fallback():
    # Leading space defeats the fast byte path but is valid JSON.
    buf = b'{ "ts-ms":2000,"event":"end","qid":"q1","status":"ok"}\n'
    assert list(scan_range(buf, 0, len(buf))) == [
        ((2000, "end", "q1", "ok"), 0),
    ]


def test_scan_range_parses_a_line_longer_than_head_bytes():
    # The query blob pushes the line well past HEAD_BYTES, but the
    # header fields sit in the head slice, so the line still parses
    # without reading the blob.
    big_query = b"x" * (log_reader.HEAD_BYTES * 4)
    start = (
        b'{"ts-ms":1000,"event":"start","qid":"q0",'
        b'"query":"' + big_query + b'"}\n'
    )
    end = b'{"ts-ms":1010,"event":"end","qid":"q0","status":"ok"}\n'
    buf = start + end
    assert list(scan_range(buf, 0, len(buf))) == [
        ((1000, "start", "q0", None), 0),
        ((1010, "end", "q0", "ok"), len(start)),
    ]


def test_scan_range_from_eof_yields_nothing():
    data, _ = build_log([1000, 1010])
    assert list(scan_range(data, len(data), len(data))) == []


def test_pair_start_end_events_empty_input_yields_empty_outputs():
    completed, still_open = pair_start_end_events(iter([]))
    assert completed == []
    assert still_open == {}


def test_pair_start_end_events_clean_pair():
    events = [
        ((1000, "start", "q1", None), 0),
        ((1050, "end", "q1", "ok"), 100),
    ]
    completed, still_open = pair_start_end_events(iter(events))
    assert completed == [
        CompletedQuery(
            start_ms=1000,
            end_ms=1050,
            duration_ms=50,
            status="ok",
            start_line_offset=0,
        )
    ]
    assert still_open == {}


def test_pair_start_end_events_start_without_end_stays_open():
    events = [((1000, "start", "q1", None), 42)]
    completed, still_open = pair_start_end_events(iter(events))
    assert completed == []
    assert still_open == {"q1": (1000, 42)}


def test_pair_start_end_events_end_without_start_is_dropped():
    events = [((1050, "end", "q1", "ok"), 0)]
    completed, still_open = pair_start_end_events(iter(events))
    assert completed == []
    assert still_open == {}


def test_pair_start_end_events_interleaved_queries_pair_in_end_order():
    events = [
        ((1000, "start", "qA", None), 0),
        ((1010, "start", "qB", None), 100),
        ((1020, "end", "qB", "ok"), 200),
        ((1030, "end", "qA", "failed"), 300),
    ]
    completed, still_open = pair_start_end_events(iter(events))
    assert completed == [
        CompletedQuery(
            start_ms=1010,
            end_ms=1020,
            duration_ms=10,
            status="ok",
            start_line_offset=100,
        ),
        CompletedQuery(
            start_ms=1000,
            end_ms=1030,
            duration_ms=30,
            status="failed",
            start_line_offset=0,
        ),
    ]
    assert still_open == {}


def test_pair_start_end_events_pairs_by_qid_not_file_order():
    # qA's end is written before qB's start (timestamps out of order),
    # but pairing is by qid so each query gets its own duration.
    events = [
        ((1000, "start", "qA", None), 0),
        ((1100, "start", "qB", None), 100),
        ((1050, "end", "qA", "ok"), 200),
        ((1150, "end", "qB", "ok"), 300),
    ]
    completed, _ = pair_start_end_events(iter(events))
    assert completed == [
        CompletedQuery(
            start_ms=1000,
            end_ms=1050,
            duration_ms=50,
            status="ok",
            start_line_offset=0,
        ),
        CompletedQuery(
            start_ms=1100,
            end_ms=1150,
            duration_ms=50,
            status="ok",
            start_line_offset=100,
        ),
    ]


@pytest.mark.parametrize("status", VALID_STATUSES)
def test_pair_start_end_events_status_flows_through(status):
    events = [
        ((1000, "start", "q1", None), 0),
        ((1050, "end", "q1", status), 100),
    ]
    completed, _ = pair_start_end_events(iter(events))
    assert completed[0].status == status


def test_extract_qid_ip_query_returns_all_three_fields():
    line = (
        b'{"ts-ms":1,"event":"start","qid":"q1","client-ip":"1.2.3.4",'
        b'"query":"SELECT * WHERE { ?s ?p ?o }"}\n'
    )
    assert extract_qid_ip_query(line) == (
        "q1",
        "1.2.3.4",
        "SELECT * WHERE { ?s ?p ?o }",
    )


def test_extract_qid_ip_query_handles_escaped_quote_in_query():
    line = (
        b'{"ts-ms":1,"event":"start","qid":"q1","client-ip":"1.2.3.4",'
        b'"query":"SELECT ?x WHERE { ?x ?p \\"a\\" }"}\n'
    )
    assert extract_qid_ip_query(line) == (
        "q1",
        "1.2.3.4",
        'SELECT ?x WHERE { ?x ?p "a" }',
    )


def test_extract_qid_ip_query_missing_client_ip_falls_back_to_empty():
    # Older log lines written before the client-ip field existed.
    line = (
        b'{"ts-ms":1,"event":"start","qid":"q1",'
        b'"query":"SELECT * WHERE { ?s ?p ?o }"}\n'
    )
    assert extract_qid_ip_query(line) == (
        "q1",
        "",
        "SELECT * WHERE { ?s ?p ?o }",
    )


def test_extract_qid_ip_query_malformed_json_returns_empty():
    assert extract_qid_ip_query(b"this is not json\n") == ("", "", "")


def query_contains(query, raw_search, ignore_case=True):
    """Build a start line around `query` and run line_query_contains."""
    line = (
        b'{"ts-ms":1,"event":"start","qid":"q77","client-ip":"1.2.3.4",'
        b'"query":"' + query + b'"}\n'
    )
    return line_query_contains(line, 0, len(line) - 1, raw_search, ignore_case)


def test_line_query_contains_finds_plain_text():
    assert query_contains(b"SELECT * WHERE { ?s ?p ?o }", b"where")


def test_line_query_contains_is_case_insensitive():
    assert query_contains(b"SELECT * WHERE { ?s ?p ?o }", b"select")


def test_line_query_contains_missing_text_is_false():
    assert not query_contains(b"SELECT * WHERE { ?s ?p ?o }", b"construct")


def test_line_query_contains_matches_escaped_quote():
    # The file holds \"berlin\"; the search is escaped the same way.
    assert query_contains(b'name \\"Berlin\\"', b'\\"berlin\\"')


def test_line_query_contains_rejects_hit_inside_escape():
    # The query is a, backslash, n, t, b: its bytes contain \nt, the
    # escaped form of newline + t, but the query has no newline.
    assert not query_contains(b"a\\\\ntb", b"\\nt")


def test_line_query_contains_skips_fake_hit_then_finds_real_one():
    # The query is a, backslash, newline, t, b: the first \nt hit
    # starts inside the \\ escape, the second is the real newline.
    assert query_contains(b"a\\\\\\ntb", b"\\nt")


def test_line_query_contains_exact_matches_same_case():
    query = "label 'Zürich'".encode()
    assert query_contains(query, "Zürich".encode(), ignore_case=False)


def test_line_query_contains_exact_rejects_other_case():
    query = "label 'Zürich'".encode()
    assert not query_contains(query, "zürich".encode(), ignore_case=False)


def test_line_query_contains_ignores_other_fields():
    # q77 appears in the qid field, not in the query.
    assert not query_contains(b"SELECT 1", b"q77")


def test_line_query_contains_ignores_line_closing_bytes():
    # The trailing "} closes the line itself, not query text.
    assert not query_contains(b"SELECT 1", b'1"}')


def test_line_query_contains_line_without_query_key_is_false():
    line = b'{"ts-ms":1,"event":"end","qid":"q1","status":"ok"}\n'
    assert not line_query_contains(line, 0, len(line) - 1, b"ok", True)


def test_load_sparql_at_returns_qid_ip_and_query():
    buf = (
        b'{"ts-ms":1,"event":"start","qid":"q1","client-ip":"1.2.3.4",'
        b'"query":"SELECT * WHERE { ?s ?p ?o }"}\n'
    )
    assert load_sparql_at(buf, 0) == (
        "q1",
        "1.2.3.4",
        "SELECT * WHERE { ?s ?p ?o }",
    )


def test_load_sparql_at_handles_escaped_quotes_and_backslashes():
    # The JSON value encodes a literal " and a literal \ in the query.
    buf = (
        b'{"ts-ms":1,"event":"start","qid":"q1","client-ip":"1.2.3.4",'
        b'"query":"SELECT ?x WHERE { ?x ?p \\"a\\\\b\\" }"}\n'
    )
    assert load_sparql_at(buf, 0) == (
        "q1",
        "1.2.3.4",
        'SELECT ?x WHERE { ?x ?p "a\\b" }',
    )


def test_load_sparql_at_handles_unicode():
    buf = (
        b'{"ts-ms":1,"event":"start","qid":"q1","client-ip":"1.2.3.4",'
        b'"query":"SELECT ?s WHERE { ?s ?p \xe2\x98\x83 }"}\n'
    )
    assert load_sparql_at(buf, 0) == (
        "q1",
        "1.2.3.4",
        "SELECT ?s WHERE { ?s ?p ☃ }",
    )


def test_load_sparql_at_malformed_json_returns_empty():
    assert load_sparql_at(b"this is not json\n", 0) == ("", "", "")


def test_load_sparql_at_reads_the_line_at_the_given_offset():
    first = (
        b'{"ts-ms":1,"event":"start","qid":"q1","client-ip":"1.2.3.4",'
        b'"query":"FIRST"}\n'
    )
    second = (
        b'{"ts-ms":2,"event":"start","qid":"q2","client-ip":"5.6.7.8",'
        b'"query":"SECOND"}\n'
    )
    assert load_sparql_at(first + second, len(first)) == (
        "q2",
        "5.6.7.8",
        "SECOND",
    )


def snippet_line(query: bytes) -> bytes:
    """A start line whose query value is the raw (escaped) `query` bytes."""
    return (
        b'{"ts-ms":1,"event":"start","qid":"q1","client-ip":"1.2.3.4",'
        b'"query":"' + query + b'"}\n'
    )


def test_load_sparql_snippet_at_short_query_decodes_in_full():
    assert load_sparql_snippet_at(
        snippet_line(b"SELECT * { ?s ?p ?o }"), 0
    ) == (
        "q1",
        "1.2.3.4",
        "SELECT * { ?s ?p ?o }",
    )


def test_load_sparql_snippet_at_handles_escapes():
    # Escaped quote, backslash, and newline in a short query.
    buf = snippet_line(b'a\\"b\\\\c\\nd')
    assert load_sparql_snippet_at(buf, 0) == ("q1", "1.2.3.4", 'a"b\\c\nd')


def test_load_sparql_snippet_at_reads_at_the_given_offset():
    first = snippet_line(b"FIRST")
    second = (
        b'{"ts-ms":2,"event":"start","qid":"q2","client-ip":"5.6.7.8",'
        b'"query":"SECOND"}\n'
    )
    assert load_sparql_snippet_at(first + second, len(first)) == (
        "q2",
        "5.6.7.8",
        "SECOND",
    )


def test_load_sparql_snippet_at_caps_a_long_query():
    # A query far past the cap is read only up to SNIPPET_BYTES chars.
    qid, client_ip, sparql = load_sparql_snippet_at(
        snippet_line(b"x" * (SNIPPET_BYTES * 4)), 0
    )
    assert (qid, client_ip) == ("q1", "1.2.3.4")
    assert sparql == "x" * SNIPPET_BYTES


def test_load_sparql_snippet_at_trims_an_escape_split_by_the_cap():
    # The cap lands on the backslash of a `\n` escape. The split escape
    # is dropped so the snippet still decodes, one char short of the cap.
    buf = snippet_line(b"a" * (SNIPPET_BYTES - 1) + b"\\n" + b"b" * 50)
    assert load_sparql_snippet_at(buf, 0) == (
        "q1",
        "1.2.3.4",
        "a" * (SNIPPET_BYTES - 1),
    )


def test_load_sparql_snippet_at_empty_query():
    assert load_sparql_snippet_at(snippet_line(b""), 0) == (
        "q1",
        "1.2.3.4",
        "",
    )


def test_read_last_timestamp_returns_last_complete_line_ts():
    assert read_last_timestamp(FIRST_LINE + SECOND_LINE) == 2000


def test_read_last_timestamp_skips_trailing_partial_line():
    partial = b'{"ts-ms":3000,"event":"end","qid":"q2","status'
    assert read_last_timestamp(FIRST_LINE + SECOND_LINE + partial) == 2000


def test_read_last_timestamp_handles_a_long_last_line():
    # rfind walks back over the whole line regardless of its size.
    long_query = b"x" * (40 * 1024)
    line = (
        b'{"ts-ms":1000,"event":"start","qid":"q1",'
        b'"query":"' + long_query + b'"}\n'
    )
    assert read_last_timestamp(line) == 1000


def test_read_first_timestamp_returns_first_complete_line_ts():
    assert read_first_timestamp(FIRST_LINE + SECOND_LINE) == 1000


def test_read_first_timestamp_single_line():
    assert read_first_timestamp(FIRST_LINE) == 1000


def test_read_first_timestamp_malformed_first_line_returns_none():
    # We read only the first line; a malformed one yields None rather
    # than scanning forward (the real log's first line is well-formed).
    assert read_first_timestamp(b"not a log line\n" + SECOND_LINE) is None


def test_read_first_timestamp_unterminated_only_line_returns_none():
    partial = b'{"ts-ms":1000,"event":"start","qid":"q1"'
    assert read_first_timestamp(partial) is None


def test_open_log_buffer_empty_file_yields_none(write_log):
    with open_log_buffer(write_log(b"")) as buf:
        assert buf is None


def test_open_log_buffer_maps_a_nonempty_file(write_log):
    with open_log_buffer(write_log(FIRST_LINE + SECOND_LINE)) as buf:
        assert read_first_timestamp(buf) == 1000
        assert read_last_timestamp(buf) == 2000
