"""Shared data models for the monitor-queries TUI.

These frozen dataclasses are the contract between the data layer and the
UI. Widgets render them; the data adapters produce them. Neither side
imports the other: both depend on this module.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class LiveSubtitle:
    """Subtitle line shown under the Live HeaderRow.

    state is one of:
      'checking'    boot, no evidence yet
      'reachable'   server confirmed alive (log fresh or ping ok)
      'pinging'     was reachable, log went quiet, silently rechecking;
                    renders the same as reachable
      'unreachable' three consecutive pings failed
    """

    endpoint: str
    state: str
    n_active: int | None


@dataclass(frozen=True)
class MetricsCounts:
    label: str
    seen: int | None
    ok: int | None
    failed: int | None
    timeout: int | None
    cancelled: int | None
    unknown: int | None
    am: int | None
    gm: int | None
    p50: int | None
    p95: int | None
    slow: int | None
    not_ready_message: str | None = None


@dataclass(frozen=True)
class LiveQueryRow:
    qid: str
    started_at_ms: int
    duration_ms: int
    sparql: str
    client_ip: str = ""


@dataclass(frozen=True)
class HistoricQueryRow:
    qid: str
    start_line_offset: int
    started_at_ms: int
    duration_ms: int
    status: str
    sparql: str
    client_ip: str = ""


@dataclass(frozen=True)
class SparqlContent:
    """What the SparqlPane renders for the row under the table cursor.

    `status` is filled at the screen seam: Live passes None (active
    queries have no terminal status), Historic passes the real status.
    """

    qid: str
    started_at_ms: int
    status: str | None
    sparql_text: str
    client_ip: str = ""


@dataclass(frozen=True)
class TimelineBounds:
    """The full log span and the slice the window currently covers.

    The Timeline maps these epochs (ms) to bar positions at render.
    """

    log_start_ms: int
    log_end_ms: int
    window_start_ms: int
    window_end_ms: int


@dataclass(frozen=True)
class ControlsState:
    window_size: str
    mode: str
    start_ms: int
    end_ms: int


@dataclass(frozen=True)
class FilterState:
    """The active filters on the Historic table.

    Empty `statuses` keeps every status; `min_duration_s` of None
    keeps any duration. A None text filter keeps every query; a set
    one keeps queries whose value contains it, ignoring case.
    Filtering hides rows but does not change the metrics.
    """

    statuses: frozenset[str] = frozenset()
    min_duration_s: int | None = None
    client_ip_substr: str | None = None
    sparql_substr: str | None = None

    def is_empty(self) -> bool:
        """Whether no filter is active, so every row passes."""
        return (
            not self.statuses
            and self.min_duration_s is None
            and self.client_ip_substr is None
            and self.sparql_substr is None
        )

    def has_text_filter(self) -> bool:
        """Whether a filter needs the query text read from the log."""
        return (
            self.client_ip_substr is not None or self.sparql_substr is not None
        )


@dataclass(frozen=True)
class ResourceSample:
    """One reading of server resource usage, as the log wrote it

    elapsed_s: seconds the server has been running, resets on restart
    ts_ms: wall-clock time of the sample
    rss: memory in bytes
    cpu_percent: CPU use, above 100 when several cores are busy
    read_bytes_per_s, write_bytes_per_s: this server's disk I/O
    io_stall_percent: share of time anything on the machine waited on disk,
      so machine-wide and not just this server
    rebuild_id: which index rebuild was running, counted from 1. None when
      no rebuild in progress.
    """

    elapsed_s: float
    ts_ms: int
    rss: int
    cpu_percent: float
    read_bytes_per_s: float | None = None
    write_bytes_per_s: float | None = None
    io_stall_percent: float | None = None
    rebuild_id: int | None = None


@dataclass(frozen=True)
class ResourceTotals:
    """Host capacities the gauges and plot axes scale against.

    Read once at startup and fixed for the machine's lifetime. cores is
    None when the count could not be read.
    """

    ram_gb: float
    cores: float | None


@dataclass(frozen=True)
class ResourceSeries:
    """One sparkline's data, already in display units.

    values is the recent series the sparkline draws, total the capacity
    it scales against, both in unit. total is None when the capacity is
    unknown (e.g. the core count could not be read). The widget renders
    this as-is and does no math of its own.
    """

    label: str
    values: tuple[float, ...]
    total: float | None
    unit: str


@dataclass(frozen=True)
class ResourceUsage:
    """The two resource sparklines shown in the Live header, as one unit."""

    rss: ResourceSeries
    cpu: ResourceSeries


@dataclass(frozen=True)
class ResourcePlot:
    """Points and frame for the dual-axis resource plot modal.

    times_s: shared x-axis, in epoch seconds
    rss_gb, cpu_cores: the two y-series, in display units
    rss_total, cpu_total: axis capacities, cpu_total None if unknown
    start_s, end_s: window edges the x-axis frames, often wider than
      the samples that fall inside them
    start_times_s: first sample after it came back up
    stop_times_s: last sample before the server went down
    rebuild_start_times_s: first sample of an index rebuild
    rebuild_end_times_s: last sample of index rebuild, finished or failed
    """

    times_s: tuple[float, ...]
    rss_gb: tuple[float, ...]
    cpu_cores: tuple[float, ...]
    rss_total: float
    cpu_total: float | None
    start_s: float
    end_s: float
    start_times_s: tuple[float, ...]
    stop_times_s: tuple[float, ...]
    rebuild_start_times_s: tuple[float, ...]
    rebuild_end_times_s: tuple[float, ...]
