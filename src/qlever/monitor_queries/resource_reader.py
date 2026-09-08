"""Reads the server's resource-usage TSV log.

This file finds the right place in the log, reads the rows from there
and turns each one into a sample. It does no math on the numbers, so
adding a plot never means touching the file handling.
"""

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

# Bytes to read from the tail per buffered row when seeding. A full row
# runs to about 80 bytes, so 96 always covers the whole window.
SEED_BYTES_PER_ROW = 96

# Backup before the bisect's landing offset, so the forward scan never
# skips the boundary line when it lands exactly on a line start.
SEEK_BACKUP_BYTES = 256

# Rows scanned between `should_cancel` polls, so a long read can abort
# without the check itself costing anything on the common short read.
CANCEL_CHECK_ROWS = 50_000

# The resource-usage log's columns in order. The optional columns can be empty
# and are only present in the new log format.
REQUIRED_COLUMNS = ("elapsed_s", "timestamp_ms", "rss", "cpu_percent")
OPTIONAL_COLUMNS = (
    "read_bytes_per_s",
    "write_bytes_per_s",
    "io_stall_percent",
    "index_rebuild_id",
)
LOG_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS


@dataclass(frozen=True)
class Sample:
    """One reading of server resource usage, as the log wrote it.

    `elapsed_s`: seconds the server has been running, resets on restart
    `ts_ms`: wall-clock time of the sample
    `rss`: memory in bytes
    `cpu_percent`: CPU use, above 100 when several cores are busy
    `read_bytes_per_s`, `write_bytes_per_s`: this server's disk I/O
    `io_stall_percent`: share of time anything on the machine waited on
      disk, so machine-wide and not just this server
    `index_rebuild_id`: which index rebuild was running, counted from 1,
      and None when no rebuild was in progress
    """

    elapsed_s: float
    ts_ms: int
    rss: int
    cpu_percent: float
    read_bytes_per_s: float | None = None
    write_bytes_per_s: float | None = None
    io_stall_percent: float | None = None
    index_rebuild_id: int | None = None


def log_has_new_columns(log_path: Path) -> bool:
    """Determine if the resource log file has the new optional columns."""
    try:
        with log_path.open() as log_file:
            header = log_file.readline()
    except OSError:
        return False
    return len(header.split("\t")) == len(LOG_COLUMNS)


def optional_cell(
    text: str, convert: Callable[[str], float | int]
) -> float | int | None:
    """Convert one optional column cell, or None if empty."""
    text = text.strip()
    return convert(text) if text else None


def parse_tsv_row(line: str) -> Sample | None:
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
        return Sample(
            elapsed_s=float(elapsed),
            ts_ms=int(ts),
            rss=int(rss),
            cpu_percent=float(cpu),
            read_bytes_per_s=optional_cell(read_bytes, float),
            write_bytes_per_s=optional_cell(write_bytes, float),
            io_stall_percent=optional_cell(io_stall, float),
            index_rebuild_id=optional_cell(rebuild_id, int),
        )
    except ValueError:
        return None


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


def iter_samples(
    stream: BinaryIO,
    start_ms: int,
    end_ms: int,
    should_cancel: Callable[[], bool] | None = None,
) -> Iterator[Sample]:
    """Yield the samples around [start_ms, end_ms], oldest first.

    Seeks near the window start rather than reading the log from the
    top, so a long log costs no more than a short one. A few rows from
    just outside the window come along as well, because the caller
    needs them to spot a restart or rebuild that began before the
    window and ended inside it.
    """
    stream.seek(0, 2)
    file_size = stream.tell()
    boundary = seek_to_window_start(stream, start_ms, file_size)
    read_from = max(0, boundary - SEEK_BACKUP_BYTES)
    stream.seek(read_from)
    if read_from > 0:
        stream.readline()
    rows_since_check = 0
    for raw in stream:
        rows_since_check += 1
        if should_cancel is not None and rows_since_check >= CANCEL_CHECK_ROWS:
            if should_cancel():
                return
            rows_since_check = 0
        sample = parse_tsv_row(raw.decode())
        if sample is None:
            continue
        yield sample
        # Stop one row past the window, not at it, so an event right at
        # the far edge still has the sample that pairs with it.
        if sample.ts_ms > end_ms:
            return


class SampleTail:
    """Follows the resource-usage log forward from a byte cursor.

    Each poll reads only the rows appended since the last one, so the
    cost stays the same however large the log grows. The worker owns the
    open stream and passes it in. `last_ts_ms` is the freshest row's
    time, which the Live screen reads as a quick sign that the server is
    alive, before the slower metrics-log and ping checks.
    """

    def __init__(self, buffered_rows: int) -> None:
        self.cursor = 0
        self.last_ts_ms = None
        self.tail_bytes = buffered_rows * SEED_BYTES_PER_ROW

    def seed(self, stream: BinaryIO, cutoff_ms: int) -> list[Sample]:
        """Backfill the last rows of the log, once, at startup.

        Seeks near the end rather than scanning from the start, so a log
        grown large over past sessions costs a fixed read, then skips
        the partial line the seek lands in. The server appends to this
        log across restarts, so the tail can hold rows from an old
        session; anything older than `cutoff_ms` is dropped.
        """
        stream.seek(0, 2)
        start = max(0, stream.tell() - self.tail_bytes)
        stream.seek(start)
        if start > 0:
            stream.readline()
        self.cursor = stream.tell()
        return [
            sample
            for sample in self.read_new(stream)
            if sample.ts_ms >= cutoff_ms
        ]

    def read_new(self, stream: BinaryIO) -> list[Sample]:
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
