"""Full-screen modal showing every resource plot at once.

The panes inside do the drawing. This modal only stacks them, frames
them and closes them. They all read the same window, so the same moment
in time is at the same place in each.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import Button, Static

from qlever.monitor_queries.models import ResourceWindow
from qlever.monitor_queries.util import action_key
from qlever.monitor_queries.widgets.detail_row import (
    GutterControl,
    gutter_control,
)
from qlever.monitor_queries.widgets.footer import Footer
from qlever.monitor_queries.widgets.resource_plot_pane import (
    Plot,
    ResourcePlotPane,
    next_step,
)
from qlever.monitor_queries.widgets.window_arrows import WindowArrows
from qlever.monitor_queries.widgets.window_range import WindowRange
from qlever.monitor_queries.widgets.window_stepper import WindowStepper


def axis_top_controls(plot: Plot) -> list[GutterControl]:
    """The raise and lower arrows for one plot's left axis top.

    The ids come from the plot's name, so each plot's arrows are its own.
    """
    slug = plot.name.lower().replace("/", "").replace(" ", "-")
    return [
        GutterControl(
            name=f"{slug}-raise-top",
            glyph="⇡",
            hint="Raise the left axis top",
            action=f"step_axis_top('{plot.name}', 1)",
        ),
        GutterControl(
            name=f"{slug}-lower-top",
            glyph="⇣",
            hint="Lower the left axis top",
            action=f"step_axis_top('{plot.name}', -1)",
        ),
    ]


class ResourcePlotModal(ModalScreen[dict[str, int]]):
    """Shows the plots this log can carry, stacked, full screen.

    Opens on the window and each plot's axis top the inline pane holds,
    so what the reader set is what the modal draws, and closes with each
    plot's axis top as it was left, which the inline pane takes over. In
    between, the screen keeps every pane up to date: Historic sends each
    window it reads, and Live sends fresh readings on its timer.
    """

    BINDINGS = [
        # One footer entry for the pair: the key that opened it closes it.
        Binding("escape", "close", "Close", key_display="esc/z"),
        Binding("z", "close", "Close", show=False),
        # A modal cuts the app's bindings, so quit and help are repeated.
        Binding("q", "app.quit", "Quit"),
        Binding("question_mark", "app.toggle_help", "Help"),
        # The window belongs to the screen beneath, so its keys run there.
        Binding("w", "forward('cycle_window')", "Window size", show=False),
        Binding(
            "W", "forward('cycle_window_back')", "Window size", show=False
        ),
        Binding(
            "left", "forward('shift_earlier')", "Shift earlier", show=False
        ),
        Binding("right", "forward('shift_later')", "Shift later", show=False),
        Binding(
            "shift+left",
            "forward('snap_start')",
            "Jump to log start",
            key_display="⇧ ←",
            show=False,
        ),
        Binding(
            "shift+right",
            "forward('snap_end')",
            "Jump to log end",
            key_display="⇧ →",
            show=False,
        ),
        Binding("up", "pick_plot(-1)", "Pick plot", show=False),
        Binding("down", "pick_plot(1)", "Pick plot", show=False),
        Binding("minus", "step_picked_top(-1)", "Plot scale", show=False),
        Binding("plus", "step_picked_top", "Plot scale", show=False),
    ]

    def __init__(
        self,
        owner: Screen,
        window: ResourceWindow,
        plots: list[Plot],
        top_steps: dict[str, int],
        shown_plot: str,
    ) -> None:
        super().__init__()
        # The screen that opened the modal. It owns the window, so the
        # window keys run there and the top row shows its window.
        self.owner = owner
        self.window = window
        self.plots = plots
        self.top_steps = top_steps
        # The raise and lower arrows of each adjustable plot, by name.
        self.controls = {
            plot.name: axis_top_controls(plot)
            for plot in plots
            if plot.adjustable_axis is not None
        }
        # The plot the keys step, by name. It starts on the plot the inline
        # pane shows when that one can step. None when no plot can step.
        names = self.pickable_plot_names
        if shown_plot in names:
            self.picked = shown_plot
        else:
            self.picked = names[0] if names else None

    @property
    def pickable_plot_names(self) -> list[str]:
        """The names of the plots the keys can pick, top to bottom.

        Only a plot whose top can step is pickable.
        """
        return [
            plot.name
            for plot in self.plots
            if plot.adjustable_axis is not None
        ]

    def compose(self) -> ComposeResult:
        with Vertical(id="resource-plot-modal"):
            with Horizontal(id="modal-top-row"):
                yield WindowStepper(
                    self.owner.window_size,
                    caption="WINDOW",
                    tooltip="Width of the time window the plots show.",
                )
                # Movable exactly when the arrow keys are, see check_action.
                yield WindowRange(
                    movable=hasattr(self.owner, "action_shift_earlier")
                )
            for plot in self.plots:
                with Horizontal(classes="modal-plot-row"):
                    # A plot with nothing to adjust keeps an empty column,
                    # so every plot starts at the same place in time.
                    with Vertical(classes="modal-gutter"):
                        if plot.adjustable_axis is not None:
                            raise_top, lower_top = self.controls[plot.name]
                            yield Static("╷", classes="axis-top-rail -above")
                            # The keys that pick this plot from the one
                            # above it and the one below it.
                            pick_down = Static(
                                "",
                                id=f"{raise_top.name}-pick-key",
                                classes="gutter-key -above",
                            )
                            pick_up = Static(
                                "",
                                id=f"{lower_top.name}-pick-key",
                                classes="gutter-key",
                            )
                            for pill in (pick_down, pick_up):
                                pill.tooltip = (
                                    "Pick this plot, so the + and - keys"
                                    " raise or lower its axis top"
                                )
                            yield pick_down
                            yield gutter_control(raise_top, "-modal")
                            yield gutter_control(lower_top, "-modal")
                            yield pick_up
                            yield Static("╵", classes="axis-top-rail -below")
                    yield ResourcePlotPane(
                        self.window,
                        plot,
                        top_steps=self.top_steps,
                        time_labels=plot is self.plots[-1],
                    )
        yield Footer(show_command_palette=False)

    def on_mount(self) -> None:
        """Follow the window the panes are handed, and label the arrows."""
        pane = self.query_one(ResourcePlotPane)

        def on_window() -> None:
            self.sync_top_row()
            # The window decides which steps still change the top. Wait
            # until the app has handed it to every pane, not just this
            # first one.
            self.call_after_refresh(self.sync_axis_top_arrows)

        self.watch(pane, "window", on_window)
        self.sync_axis_top_arrows()
        self.sync_picked()
        # Label the controls with the keys that run them; CSS decides
        # when the labels show.
        self.query_one(WindowStepper).set_help_keys(
            action_key(self, "forward('cycle_window_back')"),
            action_key(self, "forward('cycle_window')"),
        )
        window_range = self.query_one(WindowRange)
        if window_range.movable:
            window_range.set_help_keys(
                shift=(
                    action_key(self, "forward('shift_earlier')"),
                    action_key(self, "forward('shift_later')"),
                ),
                jump=(
                    action_key(self, "forward('snap_start')"),
                    action_key(self, "forward('snap_end')"),
                ),
            )
        # Every plot's pills name their keys. The pick decides which show.
        for plot in self.plots:
            if plot.adjustable_axis is not None:
                raise_top, lower_top = self.controls[plot.name]
                for pill_id, action in (
                    (f"{raise_top.name}-key", "step_picked_top"),
                    (f"{lower_top.name}-key", "step_picked_top(-1)"),
                    (f"{raise_top.name}-pick-key", "pick_plot(1)"),
                    (f"{lower_top.name}-pick-key", "pick_plot(-1)"),
                ):
                    self.query_one(f"#{pill_id}", Static).update(
                        action_key(self, action)
                    )

    def sync_top_row(self) -> None:
        """Show the screen beneath's window size, range and log edges."""
        self.query_one(WindowStepper).window_size = self.owner.window_size
        window = self.query_one(ResourcePlotPane).window
        window_range = self.query_one(WindowRange)
        window_range.show_range(
            int(window.start_s * 1000), int(window.end_s * 1000)
        )
        if window_range.movable:
            window_range.set_edges(
                at_start=self.owner.window_start_ms <= self.owner.log_start_ms,
                at_end=self.owner.window_end_ms >= self.owner.log_end_ms,
            )

    def action_step_axis_top(self, plot_name: str, direction: int) -> None:
        """Raise (1) or lower (-1) the named plot's left axis top."""
        for pane in self.query(ResourcePlotPane):
            if pane.plot.name == plot_name:
                pane.step_top(direction)
        self.sync_axis_top_arrows()

    def action_step_picked_top(self, direction: int = 1) -> None:
        """Raise (1) or lower (-1) the picked plot's left axis top."""
        if self.picked is not None:
            self.action_step_axis_top(self.picked, direction)

    def action_pick_plot(self, direction: int) -> None:
        """Pick the adjustable plot below (1) or above (-1) this one."""
        if self.picked is None:
            return
        names = self.pickable_plot_names
        index = names.index(self.picked) + direction
        if 0 <= index < len(names):
            self.picked = names[index]
            self.sync_picked()

    def sync_axis_top_arrows(self) -> None:
        """Grey out an axis top arrow with nowhere left to go."""
        for pane in self.query(ResourcePlotPane):
            if pane.plot.adjustable_axis is not None:
                raise_top, lower_top = self.controls[pane.plot.name]
                for control, direction in ((raise_top, 1), (lower_top, -1)):
                    wanted = next_step(
                        pane.window, pane.plot, pane.top_step, direction
                    )
                    glyph = self.query_one(f"#{control.name}-glyph", Button)
                    glyph.disabled = wanted is None

    def sync_picked(self) -> None:
        """Mark the picked plot, and show the keys that reach each plot.

        A pill shows only where its key does something: `+` and `-` on the
        picked plot, `↓` atop the plot below it and `↑` under the one above.
        """
        if self.picked is None:
            return
        names = self.pickable_plot_names
        picked_index = names.index(self.picked)
        for pane in self.query(ResourcePlotPane):
            pane.parent.set_class(pane.plot.name == self.picked, "-picked")
            if pane.plot.name in names:
                # 0 for the picked plot, 1 for the one below it, -1 above.
                offset = names.index(pane.plot.name) - picked_index
                raise_top, lower_top = self.controls[pane.plot.name]
                for pill_id, shown in (
                    (f"{raise_top.name}-key", offset == 0),
                    (f"{lower_top.name}-key", offset == 0),
                    (f"{raise_top.name}-pick-key", offset == 1),
                    (f"{lower_top.name}-pick-key", offset == -1),
                ):
                    self.query_one(f"#{pill_id}").set_class(shown, "-shown")

    def action_close(self) -> None:
        """Close the modal, unless a prior event already closed it.

        Each pane holds only its own plot's step, so they are gathered
        into one dict for the inline pane.
        """
        if self.is_current:
            steps = dict(self.top_steps)
            for pane in self.query(ResourcePlotPane):
                steps[pane.plot.name] = pane.top_step
            self.dismiss(steps)

    async def on_window_stepper_stepped(
        self, message: WindowStepper.Stepped
    ) -> None:
        """Step the window size when a stepper arrow is clicked."""
        if message.direction > 0:
            await self.action_forward("cycle_window")
        else:
            await self.action_forward("cycle_window_back")

    async def on_window_arrows_shifted(
        self, message: WindowArrows.Shifted
    ) -> None:
        """Shift the window when a shift arrow is clicked."""
        if message.direction > 0:
            await self.action_forward("shift_later")
        else:
            await self.action_forward("shift_earlier")

    async def on_window_arrows_jumped(
        self, message: WindowArrows.Jumped
    ) -> None:
        """Jump to a log edge when a jump arrow is clicked."""
        if message.direction > 0:
            await self.action_forward("snap_end")
        else:
            await self.action_forward("snap_start")

    def check_action(self, action: str, parameters: tuple) -> bool:
        """Keep only the window keys the screen beneath has an action for."""
        if action == "forward":
            return hasattr(self.owner, f"action_{parameters[0]}")
        return True

    async def action_forward(self, name: str) -> None:
        """Run a window action on the screen beneath, then show the result.

        The window size and edges change at once, before Historic's read
        lands, so the row is synced here and again when the read does.
        """
        await self.owner.run_action(name)
        self.sync_top_row()
