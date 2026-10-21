from __future__ import annotations

import time
from functools import partial

from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.reactive import reactive
from textual.screen import Screen
from textual.widgets import Button, Static
from textual.worker import get_current_worker

from qlever.monitor_queries.live_data import (
    PING_FAILS_TO_UNREACHABLE,
    PING_INTERVAL_S,
    current_ms,
    discard_finished_backlog,
    get_live_metrics,
    get_live_query_rows,
    get_recent_operations,
    is_log_fresh,
)
from qlever.monitor_queries.models import (
    LiveSubtitle,
    ResourceWindow,
    SparqlContent,
)
from qlever.monitor_queries.resource_data import (
    LIVE_WINDOW_PRESETS,
    SampleBuffer,
    coverage_note,
    is_sample_fresh,
    live_buffer_span_ms,
    live_window_bounds,
    window_for_samples,
)
from qlever.monitor_queries.resource_reader import (
    Sample,
    SampleTail,
)
from qlever.monitor_queries.util import action_key
from qlever.monitor_queries.views.resource_plot_modal import (
    ResourcePlotModal,
)
from qlever.monitor_queries.widgets.detail_row import DetailRow
from qlever.monitor_queries.widgets.detail_switcher import (
    PLOT_ID,
    DetailSwitcher,
)
from qlever.monitor_queries.widgets.footer import Footer
from qlever.monitor_queries.widgets.header_row import HeaderRow
from qlever.monitor_queries.widgets.metrics_row import MetricsRow
from qlever.monitor_queries.widgets.nav_pill import NavPill
from qlever.monitor_queries.widgets.query_table import LiveQueryTable
from qlever.monitor_queries.widgets.resource_plot_pane import (
    ResourcePlotPane,
)
from qlever.monitor_queries.widgets.resource_row import ResourceRow
from qlever.monitor_queries.widgets.resource_sparkline import ResourceSparkline
from qlever.monitor_queries.widgets.sparql_pane import SELECT_ROW_HINT
from qlever.monitor_queries.widgets.window_stepper import (
    WindowStepper,
    preset_ms,
)
from qlever.util import is_qlever_server_alive

TITLE = "QLever monitor-queries: Live"


class LiveScreen(Screen, inherit_bindings=False):
    """Live view: shows currently active queries tailed from the server log."""

    BINDINGS = [
        Binding("tab", "app.swap_screen", "Historic>", priority=True),
        Binding("f", "toggle_freeze", "Freeze/Unfreeze", show=False),
        Binding("w", "cycle_window", "Window size", show=False),
        Binding("W", "cycle_window_back", "Window size", show=False),
        Binding("r", "show_plot", "Resource plots", show=False),
        Binding("R", "show_plot(-1)", "Resource plots", show=False),
        Binding("minus", "step_top(-1)", "Plot scale", show=False),
        Binding("plus", "step_top", "Plot scale", show=False),
        Binding("z", "maximize_plot", "Full screen", show=False),
        Binding("s", "show_sparql", "SPARQL", show=False),
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
        # How much of the buffer the sparklines and the plot draw.
        self.window_size = LIVE_WINDOW_PRESETS[0]

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
            self.window_size,
            coverage_note(resource_window, self.app.sample_interval_s),
        )
        freeze_button = Button(
            "Freeze",
            variant="primary",
            id="freeze",
            compact=True,
            action="screen.toggle_freeze",
            tooltip="Stop the table and metrics from updating.",
        )
        # Clicking it must not take focus off the table.
        freeze_button.can_focus = False
        yield Horizontal(
            MetricsRow(
                get_live_metrics(state, slow_ms, current_ms()),
                self.app.slow_threshold,
            ),
            Static("", id="freeze-key", classes="key-pill"),
            freeze_button,
            id="metrics-freeze-row",
        )
        yield LiveQueryTable(rows)
        yield Static("", id="table-status")
        yield DetailRow(resource_window)
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
        # Label the gutter controls with the keys that run them; CSS
        # decides when the labels show.
        self.query_one(DetailRow).set_help_keys(partial(action_key, self))
        self.query_one(WindowStepper).set_help_keys(
            action_key(self, "cycle_window_back"),
            action_key(self, "cycle_window"),
        )
        self.query_one("#freeze-key", Static).update(
            action_key(self, "toggle_freeze")
        )
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
        row = self.query_one(ResourceRow)
        row.note = coverage_note(window, self.app.sample_interval_s)
        row.window = window
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
                stream,
                current_ms() - live_buffer_span_ms(self.app.sample_interval_s),
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
        """Snapshot the buffer as the rolling window the screen shows.

        The buckets sit on the clock, so between bucket edges only the
        newest one changes, and the window then moves on by one bucket.
        """
        start_ms, end_ms, buckets = live_window_bounds(
            current_ms(),
            preset_ms(self.window_size),
            self.app.sample_interval_s,
        )
        return window_for_samples(
            samples=self.resource_samples.samples,
            operations=get_recent_operations(self.app.live_state, start_ms),
            capacity=self.capacity,
            start_ms=start_ms,
            end_ms=end_ms,
            buckets=buckets,
        )

    def step_window(self, direction: int) -> None:
        """Move the window size one preset in `direction` (wraps)."""
        index = LIVE_WINDOW_PRESETS.index(self.window_size)
        self.window_size = LIVE_WINDOW_PRESETS[
            (index + direction) % len(LIVE_WINDOW_PRESETS)
        ]
        self.query_one(WindowStepper).window_size = self.window_size
        self.refresh_resource_window()

    def action_cycle_window(self) -> None:
        """Step to the next window-size preset (wraps)."""
        self.step_window(1)

    def action_cycle_window_back(self) -> None:
        """Step to the previous window-size preset (wraps)."""
        self.step_window(-1)

    def action_show_plot(self, step: int = 1) -> None:
        """Show the resource plot, or step to the next or previous one."""
        self.query_one(DetailSwitcher).show_plot(
            self.app.offered_plots(), step
        )
        self.refresh_table_status()

    def action_step_top(self, direction: int = 1) -> None:
        """Raise or lower the top of the plot's adjustable axis."""
        self.query_one(DetailSwitcher).step_top(direction)

    def action_maximize_plot(self) -> None:
        """Open the resource plot as a full-screen modal.

        Shows every plot this log can carry, on the window the inline
        pane is showing. The resource timer reaches the modal's panes as
        well, so they keep rolling.
        """
        pane = self.query_one(ResourcePlotPane)
        self.app.push_screen(
            ResourcePlotModal(
                owner=self,
                window=self.live_resource_window(),
                plots=self.app.offered_plots(),
                top_steps=pane.top_steps,
                shown_plot=pane.plot.name,
            ),
            self.query_one(DetailSwitcher).set_top_steps,
        )

    def action_show_sparql(self) -> None:
        """Switch the detail pane to the SPARQL query."""
        self.query_one(DetailSwitcher).show_sparql()
        self.refresh_table_status()

    def on_window_stepper_stepped(
        self, message: WindowStepper.Stepped
    ) -> None:
        """Resize the window when a stepper arrow is clicked."""
        self.step_window(message.direction)

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
        """Update the status line and name the way out on the button."""
        self.refresh_table_status()
        self.query_one("#freeze", Button).label = (
            "Resume" if frozen else "Freeze"
        )

    def action_toggle_freeze(self) -> None:
        """Toggle the frozen state of the live view."""
        self.frozen = not self.frozen

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
