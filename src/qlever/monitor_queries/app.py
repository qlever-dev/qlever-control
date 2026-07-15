from __future__ import annotations

import time
from pathlib import Path

from textual import work
from textual.app import App
from textual.binding import Binding
from textual.css.query import NoMatches
from textual.widgets import Select
from textual.worker import get_current_worker

from qlever.monitor_queries.live_data import (
    LIVE_HORIZON_MS,
    LiveLogReader,
    LiveState,
    current_ms,
    find_active_queries,
    load_completed_history,
)
from qlever.monitor_queries.log_reader import (
    open_log_buffer,
    read_first_timestamp,
)
from qlever.monitor_queries.util import clipboard_install_hint, copy_text
from qlever.monitor_queries.views.historic import HistoricScreen
from qlever.monitor_queries.views.live import LiveScreen
from qlever.monitor_queries.widgets.header_row import ThemeSelect
from qlever.monitor_queries.widgets.query_table import QueryTable
from qlever.monitor_queries.widgets.sparql_pane import SparqlPane, SparqlScroll
from qlever.util import pretty_printed_query


class MonitorQueriesApp(App):
    """
    Textual app for the interactive `monitor-queries` TUI

    The app has two tabs:
    1. Live view which tails an active `qlever-server` metrics.log file to
       show currently-active queries on the server plus rolling metrics
    2. Historic view which parses the log file over a given time window
       to show the queries observed in it plus metrics.

    In both tabs, the user has the option of selecting a particular query and
    view the full SPARQL query with syntax highlighting plus copy it.
    """

    CSS_PATH = "monitor.tcss"

    SCREENS = {"live": LiveScreen, "historic": HistoricScreen}

    BINDINGS = [
        ("q", "quit", "Quit/Exit"),
        ("t", "open_theme_picker", "Theme"),
        ("y", "copy_query", "Copy SPARQL"),
        ("p", "pretty_print", "Pretty print"),
        ("c", "clear_query", "Clear SPARQL"),
        Binding(
            "shift+up",
            "scroll_sparql_up",
            "Scroll SPARQL",
            key_display="⇧ ↑↓",
        ),
        Binding(
            "shift+down", "scroll_sparql_down", "Scroll SPARQL", show=False
        ),
    ]

    def __init__(
        self,
        log_file: Path,
        sparql_endpoint: str,
        timeout: int,
        slow_threshold: int,
        refresh_interval: float,
        system: str = "docker",
    ) -> None:
        super().__init__()
        self.log_file = log_file
        self.sparql_endpoint = sparql_endpoint
        self.timeout = timeout
        self.window_pad_ms = 2000 * timeout
        self.slow_threshold = slow_threshold
        self.refresh_interval = refresh_interval
        self.system = system
        self.live_state = LiveState()
        self.log_start_ms = None

    def set_log_start_ms(self) -> None:
        """Read the log's first timestamp and cache it on the app.

        Re-reads only while the value is still None (empty log at boot);
        once set, the value is immutable and subsequent calls no-op.
        """
        if self.log_start_ms is not None:
            return
        with open_log_buffer(self.log_file) as buf:
            if buf is None:
                return
            self.log_start_ms = read_first_timestamp(buf)

    def on_mount(self) -> None:
        """Boot the live engine, then open the Live screen."""
        # The ansi theme renders inconsistently across terminals.
        # Drop it so it is offered neither in the theme cycle nor
        # the command palette.
        ansi_themes = [
            theme for theme in self.available_themes if "ansi" in theme
        ]
        for theme in ansi_themes:
            self.unregister_theme(theme)
        self.boot_time_ms = current_ms()
        state, cut_offset, _ = find_active_queries(
            self.log_file, self.window_pad_ms
        )
        self.live_state = state
        self.cut_offset = cut_offset
        self.set_log_start_ms()
        self.tail_live_log()
        self.load_metrics_history()
        # Keep every header dropdown showing the active theme, however it
        # changes (a dropdown, or the command palette).
        self.watch(self, "theme", self.sync_theme_pickers)
        self.push_screen("live")

    @work(thread=True, exclusive=True, group="tail_live_log")
    def tail_live_log(self) -> None:
        """Poll the log forever, dispatching new events into LiveState."""
        tailer = LiveLogReader(
            self.log_file,
            self.live_state,
            self.cut_offset,
            self.window_pad_ms,
        )
        worker = get_current_worker()
        with self.log_file.open("rb") as log_stream:
            while not worker.is_cancelled:
                tailer.poll(log_stream)
                time.sleep(tailer.poll_interval)

    @work(thread=True, group="load_metrics_history")
    def load_metrics_history(self) -> None:
        """Prepend the hour before cut_offset into completed history so
        metrics aren't empty at boot.
        """
        load_completed_history(
            self.log_file,
            self.live_state,
            self.cut_offset,
            self.window_pad_ms,
        )
        # Backfill is done; record how far back metrics have data.
        log_start_or_boot = self.log_start_ms or self.boot_time_ms
        self.live_state.metrics_known_from_ms = max(
            log_start_or_boot, self.boot_time_ms - LIVE_HORIZON_MS
        )

    def action_swap_screen(self) -> None:
        """Toggle between Live and Historic (bound to Tab on each screen)."""
        target = "historic" if isinstance(self.screen, LiveScreen) else "live"
        if target == "historic":
            self.set_log_start_ms()
            if self.log_start_ms is None:
                self.notify(
                    "Log is empty - nothing to show in Historic yet",
                    severity="warning",
                )
                return
        self.switch_screen(target)

    def copy_to_clipboard(self, text: str) -> None:
        """Copy text to the clipboard, native tool first, OSC 52 fallback.

        A native tool that has no display to write to (e.g. xclip over
        SSH without X11 forwarding) exits non-zero, so the failure is
        itself the signal to fall back to the terminal's own OSC 52.
        """
        result = copy_text(text)
        if result is True:
            self.notify("Copied to clipboard")
            return

        super().copy_to_clipboard(text)
        if result is None:
            detail = (
                f"No clipboard tool found; copied via the terminal (OSC 52). "
                f"{clipboard_install_hint()} for a reliable copy, or check "
                "your terminal supports OSC 52."
            )
        else:
            detail = (
                "Clipboard tool failed; copied via the terminal (OSC 52). "
                "If it doesn't paste, your terminal may not support OSC 52."
            )
        self.notify(detail, severity="warning")

    def action_copy_query(self) -> None:
        """Copy the displayed query's SPARQL to the system clipboard."""
        pane = self.screen.query_one(SparqlPane)
        if pane.displayed_text is None:
            self.notify("No query selected", severity="warning")
            return
        self.copy_to_clipboard(pane.displayed_text)

    def action_pretty_print(self) -> None:
        """Toggle pretty-printed SPARQL in the pane for the selected query."""
        pane = self.screen.query_one(SparqlPane)
        if pane.content is None:
            self.notify("No query selected", severity="warning")
            return
        if pane.show_pretty:
            pane.show_pretty = False
            return
        pane.show_pretty = True
        # A cached result means the formatter already ran for this query.
        if pane.pretty_text is None:
            self.pretty_print_worker(pane, pane.content.sparql_text)

    @work(thread=True, exclusive=True, group="pretty_print")
    def pretty_print_worker(self, pane: SparqlPane, raw: str) -> None:
        """Run the blocking sparql-formatter off the UI thread."""
        result = pretty_printed_query(
            raw, show_prefixes=True, system=self.system
        )
        self.call_from_thread(self.apply_pretty_result, pane, raw, result)

    def apply_pretty_result(
        self, pane: SparqlPane, raw: str, result: str | None
    ) -> None:
        """Apply the formatter output, unless the selection moved on."""
        if pane.content is None or pane.content.sparql_text != raw:
            return
        if result is None:
            pane.show_pretty = False
            self.notify(
                "Pretty-printing failed: Docker/Podman not configured. "
                "Start `monitor-queries` with --system docker or podman, or "
                "set SYSTEM=docker or podman in the Qleverfile.",
                severity="error",
            )
            return
        pane.pretty_text = result

    def action_clear_query(self) -> None:
        """Drop the displayed query, restoring the empty-state hint."""
        pane = self.screen.query_one(SparqlPane)
        pane.content = None

    def action_scroll_sparql_up(self) -> None:
        """Scroll the overflowing SPARQL pane up one line."""
        self.screen.query_one(SparqlScroll).scroll_up()

    def action_scroll_sparql_down(self) -> None:
        """Scroll the overflowing SPARQL pane down one line."""
        self.screen.query_one(SparqlScroll).scroll_down()

    def check_action(
        self, action: str, parameters: tuple[object, ...]
    ) -> bool | None:
        """Show the scroll bindings only when the query overflows the pane.

        Returns False (hidden) rather than None (grayed) so the footer
        entry disappears entirely until there is something to scroll.
        """
        if action in ("scroll_sparql_up", "scroll_sparql_down"):
            try:
                scroll = self.screen.query_one(SparqlScroll)
            except NoMatches:
                return False
            return scroll.max_scroll_y > 0
        return True

    def action_open_theme_picker(self) -> None:
        """Open the header theme dropdown on the active screen."""
        picker = self.screen.query_one(ThemeSelect)
        picker.focus()
        picker.expanded = True

    def on_select_changed(self, event: Select.Changed) -> None:
        """Apply a picked theme, ignoring the initial selection echo."""
        if event.value is not Select.BLANK and event.value != self.theme:
            self.theme = event.value

    def on_theme_select_closed(self, event: ThemeSelect.Closed) -> None:
        """Return focus to the table once the dropdown closes."""
        self.screen.query_one(QueryTable).focus()

    def sync_theme_pickers(self, theme: str) -> None:
        """Point every header dropdown at the active theme."""
        for picker in self.query(ThemeSelect):
            if picker.value != theme:
                picker.value = theme
