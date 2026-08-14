from __future__ import annotations

from textual import events
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import Static

WINDOW_PRESETS = ("5m", "15m", "30m", "1h", "6h", "12h", "24h", "all")

UNIT_MS = {"m": 60_000, "h": 3_600_000}


def preset_ms(preset: str) -> int | None:
    """Width of a window preset in ms; None for the unbounded `all`."""
    if preset == "all":
        return None
    return int(preset[:-1]) * UNIT_MS[preset[-1]]


def available_presets(span_ms: int) -> tuple[str, ...]:
    """Presets that fit within the log span; `all` is always offered."""
    fits = tuple(
        preset
        for preset in WINDOW_PRESETS
        if preset != "all" and preset_ms(preset) <= span_ms
    )
    return fits + ("all",)


class WindowStepper(Horizontal):
    """Window-size stepper: WINDOW ◄ 15m ►.

    The arrows are separate Statics so each can be made independently
    clickable without restructuring the widget.
    """

    can_focus = False

    window_size = reactive("", init=False)

    class Stepped(Message):
        """Posted when an arrow is clicked; the screen resizes the window.

        `direction` is -1 for the left arrow, +1 for the right arrow.
        """

        def __init__(self, direction: int) -> None:
            super().__init__()
            self.direction = direction

    def __init__(self, window_size: str) -> None:
        """Hold the current window size to paint once mounted."""
        super().__init__()
        self.set_reactive(WindowStepper.window_size, window_size)

    def compose(self) -> ComposeResult:
        caption = Static("WINDOW", classes="stepper-caption")
        caption.tooltip = (
            "Width of the log time window whose queries are shown. "
            "Press w or click the arrows to resize."
        )
        yield caption
        yield Static("◄", id="window-prev", classes="stepper-arrow")
        yield Static(self.window_size, id="window-size")
        yield Static("►", id="window-next", classes="stepper-arrow")

    def watch_window_size(self, value: str) -> None:
        """Repaint the size label when the window size changes."""
        self.query_one("#window-size", Static).update(value)

    def on_click(self, event: events.Click) -> None:
        """Translate an arrow click into a Stepped message."""
        clicked = event.widget.id if event.widget else None
        if clicked == "window-prev":
            self.post_message(self.Stepped(-1))
        elif clicked == "window-next":
            self.post_message(self.Stepped(1))
