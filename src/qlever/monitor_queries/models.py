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
class ResourceSeries:
    """One log column's readings over a window, in display units.

    `key` is the log's column name, which is also the key this series
    is stored under. `total` is the capacity the bars and the axis
    scale against, in the same `unit` as `values`. It is None when that
    capacity could not be read, as happens with the core count, or when
    the column has no ceiling at all.
    """

    key: str
    label: str
    unit: str
    values: tuple[float, ...]
    total: float | None


@dataclass(frozen=True)
class ResourceEvent:
    """A moment the plot marks with a vertical line.

    `kind` is one of `server_down`, `server_up`, `rebuild_start` or
    `rebuild_end`, and the widget looks up the line's colour and label
    from it.
    """

    kind: str
    time_s: float


@dataclass(frozen=True)
class ResourceWindow:
    """One time window of resource readings, in display units.

    `times_s` holds one time per bucket that had a sample, and every
    series has one value per entry, so they all line up. `series` is
    keyed by the log's column name, and a column with no reading
    anywhere in the window is absent, which is how a plot knows it
    cannot be drawn. `start_s` and `end_s` frame the time axis and are
    often wider than the samples that fall inside them.
    """

    start_s: float
    end_s: float
    times_s: tuple[float, ...]
    series: dict[str, ResourceSeries]
    events: tuple[ResourceEvent, ...]
