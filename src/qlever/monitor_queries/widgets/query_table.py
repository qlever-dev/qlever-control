from __future__ import annotations

from rich.text import Text
from textual.widgets import DataTable

from qlever.monitor_queries.models import HistoricQueryRow, LiveQueryRow
from qlever.monitor_queries.util import (
    format_clock,
    format_duration,
    oneline,
    truncate,
)

# Full text is read in the SparqlPane.
SPARQL_WIDTH = 200


class QueryTable(DataTable):
    """Common behavior for the Live and Historic query tables.

    Each subclass defines its own columns, gives each row a key, and
    renders a row to its cells. The base uses those to fill the table
    and to keep the cursor on the same row when the rows are replaced.
    """

    def __init__(
        self, rows: list[LiveQueryRow] | list[HistoricQueryRow]
    ) -> None:
        """Hold the rows to paint once the table is mounted."""
        super().__init__(cursor_type="row")
        self.query_rows = rows

    def row_key(self, row: LiveQueryRow | HistoricQueryRow) -> str:
        """The DataTable row key identifying this query."""
        raise NotImplementedError

    def render_row(self, row: LiveQueryRow | HistoricQueryRow) -> tuple:
        """Render one query to its display cells."""
        raise NotImplementedError

    def fill_rows(self) -> None:
        """Add one keyed DataTable row per entry in self.query_rows."""
        for row in self.query_rows:
            self.add_row(*self.render_row(row), key=self.row_key(row))

    def key_under_cursor(self) -> str | int | None:
        """The key of the row under the cursor, or None when none is valid."""
        index = self.cursor_row
        if self.query_rows and 0 <= index < len(self.query_rows):
            return self.row_key(self.query_rows[index])
        return None

    def restore_cursor(
        self, key: str | int | None, fallback_index: int
    ) -> None:
        """Move the cursor back to key, or clamp to fallback_index if it is gone."""
        if not self.query_rows:
            return
        if key is not None:
            for index, row in enumerate(self.query_rows):
                if self.row_key(row) == key:
                    self.move_cursor(row=index)
                    return
        self.move_cursor(row=min(fallback_index, len(self.query_rows) - 1))

    def set_rows(
        self, rows: list[LiveQueryRow] | list[HistoricQueryRow]
    ) -> None:
        """Replace the rows by a full rebuild, preserving the cursor by key."""
        key = self.key_under_cursor()
        fallback_index = self.cursor_row
        self.query_rows = rows
        self.clear(columns=False)
        self.fill_rows()
        self.restore_cursor(key, fallback_index)


class LiveQueryTable(QueryTable):
    """Active queries on the Live screen; the only focusable widget."""

    def __init__(self, rows: list[LiveQueryRow]) -> None:
        """Track the cells and column keys the incremental diff compares."""
        super().__init__(rows)
        # qid -> the cell tuple currently shown for that row.
        self.shown_cells = {}
        # Column keys in display order, recorded when columns are added.
        self.column_keys = ()

    def on_mount(self) -> None:
        """Add the columns and one table row per active query."""
        self.column_keys = (
            self.add_column("Started"),
            self.add_column("Duration", width=8),
            self.add_column("Client IP"),
            self.add_column("SPARQL"),
        )
        self.fill_rows()

    def render_row(self, row: LiveQueryRow) -> tuple:
        """Render one active query to its Started/Duration/Client IP/SPARQL
        cells. Client IP and SPARQL are wrapped in `Text` so the table does
        not parse their free-form text as Rich markup.
        """
        return (
            format_clock(row.started_at_ms),
            Text(format_duration(row.duration_ms), justify="right"),
            Text(row.client_ip or "-"),
            Text(truncate(oneline(row.sparql), SPARQL_WIDTH)),
        )

    def row_key(self, row: LiveQueryRow) -> str:
        """Key an active query by its qid, unique among concurrent queries."""
        return row.qid

    def fill_rows(self) -> None:
        """Fill the table and remember each row's cells for the diff."""
        self.shown_cells = {}
        for row in self.query_rows:
            cells = self.render_row(row)
            self.add_row(*cells, key=row.qid)
            self.shown_cells[row.qid] = cells

    def set_rows(self, rows: list[LiveQueryRow]) -> None:
        """Bring the table to the new sorted rows by an incremental diff.

        Removes gone rows, updates only changed cells in place, and
        appends new rows at the bottom. A steady tick touches neither
        scroll nor highlight. Falls back to a full rebuild if the sorted
        order cannot be reproduced by append-at-bottom alone.
        """
        new_cells = {row.qid: self.render_row(row) for row in rows}
        new_order = [row.qid for row in rows]
        old_order = [row.qid for row in self.query_rows]
        new_set, old_set = set(new_order), set(old_order)

        survivors = [qid for qid in old_order if qid in new_set]
        additions = [qid for qid in new_order if qid not in old_set]
        if survivors + additions != new_order:
            super().set_rows(rows)
            return

        cursor_key = self.key_under_cursor()
        old_cursor_row = self.cursor_row
        layout_changed = bool(additions) or len(survivors) != len(old_order)

        for qid in old_order:
            if qid not in new_set:
                self.remove_row(qid)
        for qid in survivors:
            old_row, new_row = self.shown_cells[qid], new_cells[qid]
            for index, column_key in enumerate(self.column_keys):
                if str(old_row[index]) != str(new_row[index]):
                    self.update_cell(qid, column_key, new_row[index])
        for qid in additions:
            self.add_row(*new_cells[qid], key=qid)

        self.query_rows = rows
        self.shown_cells = new_cells
        if layout_changed and cursor_key is not None:
            self.restore_cursor(cursor_key, old_cursor_row)


class HistoricQueryTable(QueryTable):
    """Finished queries in the current window on the Historic screen."""

    # Wide enough to fit a sortable header plus its sort arrow.
    SORTABLE_COLUMN_WIDTH = 10

    def on_mount(self) -> None:
        """Add the columns and one table row per finished query."""
        self.add_column("Started", width=self.SORTABLE_COLUMN_WIDTH)
        self.add_column("Duration", width=self.SORTABLE_COLUMN_WIDTH)
        self.add_column("Status", width=self.SORTABLE_COLUMN_WIDTH)
        self.add_column("Client IP")
        self.add_column("SPARQL")
        self.fill_rows()

    def render_row(self, row: HistoricQueryRow) -> tuple:
        """Render one finished query to Started/Duration/Status/Client IP/SPARQL
        cells. Client IP and SPARQL are wrapped in `Text` so the table does
        not parse their free-form text as Rich markup.
        """
        return (
            format_clock(row.started_at_ms),
            Text(format_duration(row.duration_ms), justify="right"),
            row.status,
            Text(row.client_ip or "-"),
            Text(truncate(oneline(row.sparql), SPARQL_WIDTH)),
        )

    def row_key(self, row: HistoricQueryRow) -> str:
        """Key a finished query by its start line offset, a stable identity.

        A window can hold the same qid twice (the server reuses a qid
        after its end), but every start line sits at one byte offset.
        """
        return str(row.start_line_offset)

    def set_sort_indicator(self, column_index: int, descending: bool) -> None:
        """Show a sort arrow on one column header and clear the rest."""
        arrow = " ▼" if descending else " ▲"
        for index, column in enumerate(self.ordered_columns):
            base = column.label.plain.rstrip(" ▲▼")
            label = base + arrow if index == column_index else base
            column.label = Text(label)
        # Invalidate the render cache so the new labels paint, the way
        # update_cell does; Textual has no public column relabel.
        self._update_count += 1
        self.refresh()
