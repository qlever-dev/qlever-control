from __future__ import annotations

import time

from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.reactive import reactive
from textual.screen import Screen
from textual.widgets import Static
from textual.worker import get_current_worker

from qlever.monitor_queries.live_data import (
    PING_FAILS_TO_UNREACHABLE,
    PING_INTERVAL_S,
    current_ms,
    discard_finished_backlog,
    get_live_metrics,
    get_live_query_rows,
    is_log_fresh,
)
from qlever.monitor_queries.models import (
    LiveSubtitle,
    ResourceWindow,
    SparqlContent,
)
from qlever.monitor_queries.resource_data import (
    LIVE_RESOURCE_WINDOW_MS,
    SampleBuffer,
    is_sample_fresh,
    window_for_samples,
)
from qlever.monitor_queries.resource_reader import (
    Sample,
    SampleTail,
    log_has_new_columns,
)
from qlever.monitor_queries.views.resource_plot_modal import (
    ResourcePlotModal,
)
from qlever.monitor_queries.widgets.detail_switcher import (
    PLOT_ID,
    DetailSwitcher,
)
from qlever.monitor_queries.widgets.footer import Footer
from qlever.monitor_queries.widgets.header_row import HeaderRow
from qlever.monitor_queries.widgets.metrics_row import MetricsRow
from qlever.monitor_queries.widgets.nav_pill import NavPill
from qlever.monitor_queries.widgets.query_table import LiveQueryTable
from qlever.monitor_queries.widgets.resource_plot_pane import available_plots
from qlever.monitor_queries.widgets.resource_row import ResourceRow
from qlever.monitor_queries.widgets.resource_sparkline import ResourceSparkline
from qlever.monitor_queries.widgets.sparql_pane import SELECT_ROW_HINT
from qlever.util import is_qlever_server_alive

TITLE = "QLever monitor-queries: Live"


class LiveScreen(Screen, inherit_bindings=False):
    """Live view: shows currently active queries tailed from the server log."""

    BINDINGS = [
        Binding("tab", "app.swap_screen", "Historic>", priority=True),
        Binding("f", "toggle_freeze", "Freeze/Unfreeze"),
        # One footer entry for both: r shows the pane and steps through
        # the plots, R opens the modal.
        Binding("r", "show_plot", "Resource plots", key_display="r/R"),
        Binding("R", "maximize_plot", "Maximize plot", show=False),
        Binding("s", "show_sparql", "SPARQL"),
        Binding("ctrl+c,super+c", "screen.copy_text", "Copy selection"),
    ]

    frozen = reactive(False, init=False)

    def __init__(self) -> None:
        """Set the screen's blank default state.

        The app-derived fields (liveness, capacity, the resource samples
        and the log tail) are read in compose, where the app is
        available.
        """
        super().__init__()
        self.consecutive_ping_fails = 0
        self.ping_timer = None

    def compose(self) -> ComposeResult:
        yield HeaderRow(
            left=NavPill("Historic >", target="historic"),
            center=Static(TITLE),
        )
        state = self.app.live_state
        slow_ms = self.app.slow_threshold * 1000
        rows = sorted(
            get_live_query_rows(state, current_ms()),
            key=lambda row: row.duration_ms,
            reverse=True,
        )

        self.liveness = (
            "reachable" if is_log_fresh(state, current_ms()) else "checking"
        )
        self.capacity = self.app.capacity
        self.resource_samples = SampleBuffer(self.app.sample_interval_s)
        self.resource_log_tail = SampleTail(self.resource_samples.size)

        resource_window = self.live_resource_window()
        yield ResourceRow(
            LiveSubtitle(
                endpoint=self.app.sparql_endpoint,
                state=self.liveness,
                n_active=len(rows),
            ),
            resource_window,
        )
        yield MetricsRow(
            get_live_metrics(state, slow_ms, current_ms()),
            self.app.slow_threshold,
        )
        yield LiveQueryTable(rows)
        yield Static("", id="table-status")
        yield DetailSwitcher(resource_window)
        yield Footer()

    def on_mount(self) -> None:
        """Start periodic refreshes; kick off boot pings if the log is stale."""
        self.table_timer = self.set_interval(
            self.app.refresh_interval, self.refresh_table
        )
        self.metrics_timer = self.set_interval(2.0, self.refresh_metrics)
        self.resource_timer = self.set_interval(
            self.app.sample_interval_s, self.refresh_resource_window
        )
        # A worker, not a paused-on-suspend timer: it keeps reading the
        # log regardless of the active tab so the history never gaps.
        self.tail_resource_log()
        if self.liveness == "checking":
            self.start_pinging(initial=True)
        # Focus the table so the header theme dropdown can't take it.
        self.query_one(LiveQueryTable).focus()
        self.refresh_table_status()

    def on_screen_suspend(self) -> None:
        """Pause periodic UI work while Live isn't the active screen."""
        self.table_timer.pause()
        self.metrics_timer.pause()
        if self.ping_timer is not None:
            self.ping_timer.pause()
        # Maximizing the plot also suspends Live, but leaves it on the
        # stack, and that plot is on screen and still ours to roll.
        if self in self.app.screen_stack:
            return
        self.resource_timer.pause()

    def on_screen_resume(self) -> None:
        """Resume periodic UI work; recheck server unless already reachable."""
        # Drop the backlog of queries that finished while suspended.
        discard_finished_backlog(self.app.live_state)
        self.table_timer.resume()
        self.resource_timer.resume()
        self.update_liveness_visuals()
        if self.ping_timer is not None:
            self.ping_timer.resume()
        if self.liveness != "reachable":
            self.ping_server()

    def start_pinging(self, initial: bool) -> None:
        """Enter the ping cycle when the log goes quiet or at a cold boot.

        `initial` distinguishes boot's "checking" subtitle from the
        invisible "pinging" recheck that follows a reachable period.
        Fires one ping immediately so the first verification doesn't
        wait the full interval.
        """
        self.liveness = "checking" if initial else "pinging"
        self.consecutive_ping_fails = 0
        if self.ping_timer is None:
            self.ping_timer = self.set_interval(
                PING_INTERVAL_S, self.ping_server
            )
        self.ping_server()
        self.refresh_subtitle()

    def mark_reachable(self) -> None:
        """Confirm the server is alive and tear down the ping cycle."""
        self.liveness = "reachable"
        self.consecutive_ping_fails = 0
        if self.ping_timer is not None:
            self.ping_timer.stop()
            self.ping_timer = None
        self.refresh_subtitle()
        self.update_liveness_visuals()

    def update_liveness_visuals(self) -> None:
        """Sync the dim style and metric freeze with self.liveness."""
        stale = self.liveness == "unreachable"
        self.query_one(LiveQueryTable).set_class(stale, "stale")
        self.query_one(MetricsRow).set_class(stale, "stale")
        if stale:
            self.metrics_timer.pause()
        else:
            self.metrics_timer.resume()

    @work(thread=True, exclusive=True, group="ping_server")
    def ping_server(self) -> None:
        """Curl the server's /ping off the UI thread."""
        ok = is_qlever_server_alive(self.app.sparql_endpoint, max_time=2)
        self.app.call_from_thread(self.apply_ping_result, ok)

    def apply_ping_result(self, ok: bool) -> None:
        """Advance the state machine using one ping outcome."""
        if self.liveness == "reachable":
            return
        if is_log_fresh(self.app.live_state, current_ms()):
            self.mark_reachable()
            return
        if ok:
            self.mark_reachable()
            return
        self.consecutive_ping_fails += 1
        if self.consecutive_ping_fails >= PING_FAILS_TO_UNREACHABLE:
            self.liveness = "unreachable"
        self.refresh_subtitle()
        self.update_liveness_visuals()

    def refresh_subtitle(self) -> None:
        """Rebuild the subtitle to match the table currently on screen."""
        rows = self.query_one(LiveQueryTable).query_rows
        self.query_one(ResourceRow).subtitle = LiveSubtitle(
            endpoint=self.app.sparql_endpoint,
            state=self.liveness,
            n_active=len(rows),
        )

    def update_liveness_from_log(self) -> None:
        """Re-evaluate reachability: a fresh resource sample first, then
        log freshness.

        Runs on each refresh_table tick. A growing resource log or a
        fresh log line flips back to reachable from any non-reachable
        state; losing both moves us from reachable into pinging. The
        sample check is first and short-circuits, so a live local server
        skips the log read entirely. The same sample-freshness check
        marks the resource row stale.
        """
        now = current_ms()
        samples_fresh = is_sample_fresh(
            self.resource_log_tail.last_ts_ms, now, self.app.sample_interval_s
        )
        self.query_one(ResourceRow).stale = not samples_fresh
        alive = samples_fresh or is_log_fresh(self.app.live_state, now)
        if self.liveness == "reachable" and not alive:
            self.start_pinging(initial=False)
        elif self.liveness != "reachable" and alive:
            self.mark_reachable()

    def display_clock_ms(self) -> int:
        """The clock used for live duration math.

        When unreachable, freeze at the last log timestamp so durations
        stop ticking on stale rows; otherwise trust real time.
        """
        state = self.app.live_state
        if (
            self.liveness == "unreachable"
            and state.latest_event_ms is not None
        ):
            return state.latest_event_ms
        return current_ms()

    def refresh_table(self) -> None:
        """Push the active rows sorted by duration; no-op when frozen."""
        self.update_liveness_from_log()
        if self.frozen:
            return
        state = self.app.live_state
        rows = sorted(
            get_live_query_rows(state, self.display_clock_ms()),
            key=lambda row: row.duration_ms,
            reverse=True,
        )
        self.query_one(LiveQueryTable).set_rows(rows)
        self.refresh_subtitle()

    def refresh_metrics(self) -> None:
        """Push fresh metric snapshots into MetricsRow; no-op when frozen."""
        if self.frozen:
            return
        state = self.app.live_state
        slow_ms = self.app.slow_threshold * 1000
        self.query_one(MetricsRow).rows = get_live_metrics(
            state, slow_ms, current_ms()
        )

    def refresh_resource_window(self) -> None:
        """Roll the window forward into the sparklines and the plot.

        Built once and handed to both, so the sparklines and the plot
        always show the same readings. Freezing the table leaves this
        running, since the window is about the server and not the rows.
        """
        window = self.live_resource_window()
        self.query_one(ResourceRow).window = window
        self.app.push_resource_window(window)

    @work(thread=True, exclusive=True, group="tail_resource_log")
    def tail_resource_log(self) -> None:
        """Read the resource log off the UI thread, seeding then tailing.

        I/O and parsing stay on this thread; only the buffer update and
        repaint are handed to the UI thread, so a slow read can never
        freeze the screen.
        """
        worker = get_current_worker()
        interval_s = self.app.sample_interval_s
        # The log may not exist yet: the server has not started, or
        # resource logging is off. Wait for it instead of crashing the
        # worker; the sparklines stay empty until it appears.
        while not worker.is_cancelled and not self.app.resource_log.exists():
            time.sleep(interval_s)
        if worker.is_cancelled:
            return
        with self.app.resource_log.open("rb") as stream:
            seeded = self.resource_log_tail.seed(
                stream, current_ms() - LIVE_RESOURCE_WINDOW_MS
            )
            self.app.call_from_thread(self.apply_resource_samples, seeded)
            while not worker.is_cancelled:
                fresh = self.resource_log_tail.read_new(stream)
                if fresh:
                    self.app.call_from_thread(
                        self.apply_resource_samples, fresh
                    )
                time.sleep(interval_s)

    def apply_resource_samples(self, samples: list[Sample]) -> None:
        """Add new samples to the buffer, on the UI thread.

        Runs there so the buffer is never written while the resource
        timer is reading it. Repainting is that timer's job.
        """
        for sample in samples:
            self.resource_samples.add(sample)

    def live_resource_window(self) -> ResourceWindow:
        """Snapshot the buffer as the rolling 5-minute window.

        One bucket per sampling interval, so the sparklines and the plot
        keep every reading the buffer holds. The clock is read once, so
        the window's start and end are the same instant.
        """
        now_ms = current_ms()
        return window_for_samples(
            self.resource_samples.samples,
            self.capacity,
            now_ms - LIVE_RESOURCE_WINDOW_MS,
            now_ms,
            self.resource_samples.size,
        )

    def action_show_plot(self) -> None:
        """Show the resource plot, or step to the next one.

        The log's format is read now rather than at startup, since the
        server may have begun writing it after this screen opened.
        """
        offered = available_plots(log_has_new_columns(self.app.resource_log))
        self.query_one(DetailSwitcher).show_plot(offered)
        self.refresh_table_status()

    def action_maximize_plot(self) -> None:
        """Open the resource plot as a full-screen modal.

        Shows every plot this log can carry, on the window the inline
        pane is showing. The resource timer reaches the modal's panes as
        well, so they keep rolling.
        """
        self.app.push_screen(
            ResourcePlotModal(
                self.live_resource_window(),
                available_plots(log_has_new_columns(self.app.resource_log)),
            )
        )

    def action_show_sparql(self) -> None:
        """Switch the detail pane to the SPARQL query."""
        self.query_one(DetailSwitcher).show_sparql()
        self.refresh_table_status()

    def on_resource_sparkline_clicked(
        self, message: ResourceSparkline.Clicked
    ) -> None:
        """Show the plot on click; maximize it if it is already shown."""
        if self.query_one(DetailSwitcher).current == PLOT_ID:
            self.action_maximize_plot()
        else:
            self.action_show_plot()

    def refresh_table_status(self) -> None:
        """Compose the table status line from the active hints.

        The freeze hint when frozen, and the select-a-row hint whenever
        the plot hides the SPARQL pane, so both can show at once.
        """
        hints = []
        if self.frozen:
            hints.append("Frozen - press f to resume")
        if self.query_one(DetailSwitcher).current == PLOT_ID:
            hints.append(SELECT_ROW_HINT)
        self.query_one("#table-status", Static).update(" · ".join(hints))

    def watch_frozen(self, frozen: bool) -> None:
        """Refresh the status line to reflect the frozen state."""
        self.refresh_table_status()

    def action_toggle_freeze(self) -> None:
        """Toggle the frozen state of the live view."""
        self.frozen = not self.frozen

    def on_resize(self) -> None:
        """Re-evaluate the conditional scroll bindings after a resize."""
        self.call_after_refresh(self.refresh_bindings)

    def on_nav_pill_clicked(self, message: NavPill.Clicked) -> None:
        """Switch to the screen named by the clicked pill.

        Routes through `action_swap_screen` when the pill points at the
        other tab so the empty-log guard for Historic entry stays in one
        place; any other target falls through to a plain screen switch.
        """
        if message.target == "historic":
            self.app.action_swap_screen()
        else:
            self.app.switch_screen(message.target)

    def on_data_table_row_selected(
        self, message: LiveQueryTable.RowSelected
    ) -> None:
        """Show the selected active query's SPARQL in the detail pane."""
        row = message.data_table.query_rows[message.cursor_row]
        detail = self.query_one(DetailSwitcher)
        detail.set_sparql(
            SparqlContent(
                qid=row.qid,
                started_at_ms=row.started_at_ms,
                status=None,
                sparql_text=row.sparql,
                client_ip=row.client_ip,
            )
        )
        detail.show_sparql()
        self.refresh_table_status()
