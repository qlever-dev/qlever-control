"""Pure primitives for reading the query metrics log"""

import json
import mmap
import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import NamedTuple

TS_PREFIX = b'{"ts-ms":'
EVENT_KEY = b'"event":"'
QID_KEY = b'"qid":"'
CLIENT_IP_KEY = b'"client-ip":"'
STATUS_KEY = b'"status":"'
QUERY_KEY = b'"query":"'

# Statuses the server writes; anything else maps to UNKNOWN_STATUS.
STATUS_SET = frozenset({"ok", "failed", "cancelled", "timeout"})

UNKNOWN_STATUS = "unknown"

# The log buffer: an mmap in normal use, plain bytes in tests.
LogBuffer = mmap.mmap | bytes

# Bytes read from the start of a line to get its header fields
# (timestamp, event, qid), skipping the large query text after them.
HEAD_BYTES = 256

# Query bytes read for the table, where the text is truncated to ~200
# display chars. The extra leaves room for escapes and whitespace.
SNIPPET_BYTES = 256


@contextmanager
def open_log_buffer(path: Path) -> Iterator[mmap.mmap | None]:
    """Open the log file for reading and yield it as a buffer.

    Yields None if the file is empty, since you cannot mmap an empty
    file. Callers should check `if buf is None` before using it.
    """
    with path.open("rb") as f:
        if os.fstat(f.fileno()).st_size == 0:
            yield None
            return
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as buf:
            yield buf


class CompletedQuery(NamedTuple):
    """One start event paired with its matching end event.

    start_line_offset is the byte offset of the start line in the log,
    kept so load_sparql_at can fetch the SPARQL text later. None when
    the pair was built without scanning the file (the live tailer),
    since those completions feed metrics only and never need their
    SPARQL text re-read.
    """

    start_ms: int
    end_ms: int
    duration_ms: int
    status: str
    start_line_offset: int | None


def slice_string_value(line_bytes: bytes, key: bytes) -> str | None:
    """Return the string value following `key`, up to the next quote.

    `key` includes the value's opening quote (for example b'"qid":"').
    Returns None if the key or its closing quote is absent.
    """
    start = line_bytes.find(key)
    if start == -1:
        return None
    start += len(key)
    end = line_bytes.find(b'"', start)
    if end == -1:
        return None
    return line_bytes[start:end].decode()


def peek_ts_ms(line_bytes: bytes) -> int | None:
    """Read only the leading ts-ms timestamp from a log line.

    Every log line begins with ts-ms, so the integer sits between
    the fixed prefix and the next comma. Lets navigation compare
    timestamps without parsing the rest of the line. Returns None if the
    prefix is absent or the value is not an integer.
    """
    if not line_bytes.startswith(TS_PREFIX):
        return None
    comma = line_bytes.find(b",", len(TS_PREFIX))
    if comma == -1:
        return None
    try:
        return int(line_bytes[len(TS_PREFIX) : comma])
    except ValueError:
        return None


def normalize_status(status: str) -> str:
    """Pass a known status through, map anything else to unknown."""
    return status if status in STATUS_SET else UNKNOWN_STATUS


def parse_line(
    line_bytes: bytes,
) -> tuple[int, str, str, str | None] | None:
    """Byte-slice one log line into (ts_ms, event, qid, status).

    Avoids json.loads and never scans the query blob, so the common
    line stays cheap to parse. `status` is None on start lines. Returns
    None on anything unexpected so the caller can fall back to
    parse_line_fallback. Never raises.
    """
    ts_ms = peek_ts_ms(line_bytes)
    if ts_ms is None:
        return None

    event = slice_string_value(line_bytes, EVENT_KEY)
    if event not in ("start", "end"):
        return None

    qid = slice_string_value(line_bytes, QID_KEY)
    if qid is None:
        return None

    if event == "start":
        return ts_ms, event, qid, None

    status = slice_string_value(line_bytes, STATUS_KEY)
    if status is None:
        return None
    return ts_ms, event, qid, normalize_status(status)


def parse_line_fallback(
    line_bytes: bytes,
) -> tuple[int, str, str, str | None] | None:
    """Full json.loads for a line parse_line rejected.

    Same 4-tuple shape as parse_line so callers treat both alike.
    Returns None on a malformed line or a missing/wrong field; never
    raises.
    """
    try:
        obj = json.loads(line_bytes)
    except (ValueError, TypeError):
        return None
    if not isinstance(obj, dict):
        return None

    ts_ms = obj.get("ts-ms")
    event = obj.get("event")
    qid = obj.get("qid")
    if not isinstance(ts_ms, int):
        return None
    if event not in ("start", "end"):
        return None
    if not isinstance(qid, str):
        return None

    if event == "start":
        return ts_ms, event, qid, None

    status = obj.get("status")
    if not isinstance(status, str):
        return None
    return ts_ms, event, qid, normalize_status(status)


def next_whole_line(buf: LogBuffer, probe: int) -> tuple[int, int] | None:
    """Find the first whole line at or after `probe`.

    A probe usually lands mid-line, so we move to the next line start.
    Returns (line_start, ts_ms), or None if there is no whole line
    after `probe`.
    """
    if probe >= len(buf):
        return None
    if probe == 0:
        line_start = 0
    else:
        # Skip the partial line the probe landed in.
        newline = buf.find(b"\n", probe)
        if newline == -1:
            return None
        line_start = newline + 1

    end = buf.find(b"\n", line_start)
    # No newline left means this is the file's trailing partial line.
    if end == -1:
        return None
    ts_ms = peek_ts_ms(buf[line_start : line_start + HEAD_BYTES])
    if ts_ms is None:
        return None
    return line_start, ts_ms


def read_first_timestamp(buf: LogBuffer) -> int | None:
    """Return the timestamp of the first whole line, or None."""
    end = buf.find(b"\n")
    if end == -1:
        return None
    return peek_ts_ms(buf[:HEAD_BYTES])


def read_last_timestamp(buf: LogBuffer) -> int | None:
    """Return the timestamp of the last whole line, or None.

    Walks backward from the end, skipping a trailing partial line and
    any malformed lines.
    """
    end = buf.rfind(b"\n")
    if end == -1:
        return None
    while end > 0:
        start = buf.rfind(b"\n", 0, end) + 1
        ts_ms = peek_ts_ms(buf[start : start + HEAD_BYTES])
        if ts_ms is not None:
            return ts_ms
        end = start - 1
    return None


GALLOP_START = 128 * 1024

# Re-check should_cancel after this many scanned bytes.
CANCEL_CHECK_BYTES = 8 * 1024 * 1024


def offset_for_ts(buf: LogBuffer, target_ms: int) -> int:
    """Find where to start reading so a forward read sees every line
    at or after target_ms.

    The log is only roughly time-ordered, so this aims a little early
    on purpose rather than risk skipping lines; the caller ignores the
    few extra older lines. Returns 0 if target_ms is at or before the
    first line, and a spot near the end if it is past the last line.
    """
    file_size = len(buf)
    first = next_whole_line(buf, 0)
    if first is None:
        return 0
    first_start, first_ts = first
    if first_ts >= target_ms:
        return 0

    # Target past the last line: start there, skip the gallop.
    last_newline = buf.rfind(b"\n")
    last_start = buf.rfind(b"\n", 0, last_newline) + 1
    last_ts = peek_ts_ms(buf[last_start : last_start + HEAD_BYTES])
    if last_ts is not None and target_ms > last_ts:
        return last_start

    # Gallop backward from the end, doubling the step until a probed
    # line is old enough (ts <= target). before_target brackets the
    # search from the old side, after_target from the new side.
    before_target = first_start
    after_target = file_size
    step = GALLOP_START
    while True:
        probe = file_size - step
        if probe <= before_target:
            break
        found = next_whole_line(buf, probe)
        if found is None:
            step *= 2
            continue
        start, ts = found
        if ts <= target_ms:
            before_target = start
            break
        after_target = start
        step *= 2

    # Narrow the bracket. Bias every step toward the older side so the
    # result never sits past the first matching line.
    while after_target - before_target > 1:
        mid = (before_target + after_target) // 2
        found = next_whole_line(buf, mid)
        if found is None:
            after_target = mid
            continue
        start, ts = found
        # The probe sat inside one long line spanning the bracket edge,
        # so it gave no usable interior line. Tighten the new side to
        # keep the search making progress.
        if start <= before_target or start >= after_target:
            after_target = mid
            continue
        if ts < target_ms:
            before_target = start
        else:
            after_target = start
    return before_target


def scan_range(
    buf: LogBuffer,
    lo_offset: int,
    hi_bound: int,
    should_cancel: Callable[[], bool] | None = None,
) -> Iterator:
    """Yield (parsed, line_offset) for whole lines in [lo_offset, hi_bound].

    parsed is the (ts_ms, event, qid, status) tuple. lo_offset must be
    line-aligned; it always comes from offset_for_ts. The line straddling
    hi_bound is included; a trailing line without a newline is left for a
    later read. Malformed lines are skipped.

    Only the line's head is parsed, so the large query text is never
    read. The rare line that needs more falls back to a full parse.

    When should_cancel is given, it is polled every CANCEL_CHECK_BYTES and
    the scan returns early if it is true. That yields a partial stream, so
    a caller that cancels must re-check before trusting the result.
    """
    offset = lo_offset
    next_cancel_check = lo_offset + CANCEL_CHECK_BYTES
    while offset <= hi_bound:
        newline = buf.find(b"\n", offset)
        # No newline left means a trailing partial line; leave it.
        if newline == -1:
            return
        if should_cancel is not None and offset >= next_cancel_check:
            if should_cancel():
                return
            next_cancel_check = offset + CANCEL_CHECK_BYTES
        head = buf[offset : min(newline, offset + HEAD_BYTES)]
        parsed = parse_line(head)
        if parsed is None:
            parsed = parse_line_fallback(buf[offset:newline])
        if parsed is not None:
            yield parsed, offset
        offset = newline + 1


def pair_start_end_events(
    events: Iterator,
) -> tuple[list[CompletedQuery], dict[str, tuple[int, int]]]:
    """Pair start and end events from a scan into completed queries.

    Walks the events once. Each end pops its matching start by qid into
    completed_queries; whatever remains unmatched is still_open. Unmatched
    ends are dropped (their start was outside the scanned range).

    events: yields ((ts_ms, event, qid, status), line_offset) from
    scan_range.

    Returns (completed_queries, still_open). still_open maps qid to
    (start_ms, start_line_offset) for queries with no end event seen yet.
    """
    completed_queries = []
    still_open = {}
    for (ts_ms, event, qid, status), line_offset in events:
        if event == "start":
            still_open[qid] = (ts_ms, line_offset)
            continue
        # event == "end"
        matched_start = still_open.pop(qid, None)
        if matched_start is None:
            continue
        start_ms, start_line_offset = matched_start
        completed_queries.append(
            CompletedQuery(
                start_ms=start_ms,
                end_ms=ts_ms,
                duration_ms=ts_ms - start_ms,
                status=status,
                start_line_offset=start_line_offset,
            )
        )
    return completed_queries, still_open


def extract_qid_ip_query(line_bytes: bytes) -> tuple[str, str, str]:
    """Return (qid, client_ip, query) from a start line, or ("", "", "").

    All three fields come from one json.loads. The caller has already
    validated this is a start line via parse_line, so qid and query are
    guaranteed present strings. client-ip falls back to "" so log lines
    written before the field existed still produce usable rows.
    """
    try:
        obj = json.loads(line_bytes)
    except (ValueError, TypeError):
        return "", "", ""
    return obj["qid"], obj.get("client-ip", ""), obj["query"]


def line_query_contains(
    buf: LogBuffer,
    line_offset: int,
    line_end: int,
    raw_search: bytes,
    ignore_case: bool,
) -> bool:
    """Whether the start line's query text contains the raw search.

    `raw_search` must be JSON-escaped like the log. When `ignore_case`
    is true both it and the value are lowercased for a case-insensitive
    match; when false both stay as-is for an exact match. A hit
    preceded by an odd number of backslashes sits inside an escape
    sequence, so it is skipped.
    """
    value_start = buf.find(QUERY_KEY, line_offset, line_end)
    if value_start == -1:
        return False
    value_start += len(QUERY_KEY)
    value = buf[value_start : max(line_end - 2, value_start)]
    if ignore_case:
        value = value.lower()
    position = value.find(raw_search)
    while position != -1:
        backslashes = 0
        probe = position - 1
        while probe >= 0 and value[probe : probe + 1] == b"\\":
            backslashes += 1
            probe -= 1
        if backslashes % 2 == 0:
            return True
        position = value.find(raw_search, position + 1)
    return False


def load_sparql_at(buf: LogBuffer, line_offset: int) -> tuple[str, str, str]:
    """Return (qid, client_ip, sparql) for the start line at line_offset.

    Used by callers that have only the offset, not the line bytes:
    find_active_queries on survivors (qid already known, ignored), and
    Historic on displayed rows (needs all three for the SparqlPane).
    """
    end = buf.find(b"\n", line_offset)
    line = buf[line_offset:] if end == -1 else buf[line_offset:end]
    return extract_qid_ip_query(line)


def load_sparql_snippet_at(
    buf: LogBuffer, line_offset: int
) -> tuple[str, str, str]:
    """Read qid, client-ip, and the start of the query.

    The table only shows a short, truncated query, so reading a large
    one in full is wasted. This reads just the start of it instead.
    """
    head = buf[line_offset : line_offset + HEAD_BYTES]
    qid = slice_string_value(head, QID_KEY) or ""
    client_ip = slice_string_value(head, CLIENT_IP_KEY) or ""

    key_at = buf.find(QUERY_KEY, line_offset)
    if key_at == -1:
        return qid, client_ip, ""
    value_start = key_at + len(QUERY_KEY)
    limit = value_start + SNIPPET_BYTES
    # Bounded so a large query is never scanned past the cap.
    end = buf.find(b"\n", line_offset, limit + 2)
    if end == -1:
        value = buf[value_start:limit]
    else:
        # Drop the line's closing quote and brace.
        value = buf[value_start : end - 2]

    # The cap can split an escape, so retry shorter until it decodes.
    while value:
        try:
            return qid, client_ip, json.loads(b'"' + value + b'"')
        except ValueError:
            value = value[:-1]
    return qid, client_ip, ""
