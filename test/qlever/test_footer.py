"""Tests for the rebuild-skipping Footer of monitor-queries.

The stock Textual Footer rebuilds all of its key widgets on every bindings
refresh (fired, among others, on every terminal focus change) and leaks
the replaced widgets. The subclass skips rebuilds that would draw the same
rows. These tests pin down the three behaviors that matter: redundant
refreshes cause no rebuild, a real change still rebuilds, and a change
arriving while the terminal is unfocused is drawn on refocus.
"""

import asyncio

from textual import events
from textual.app import App
from textual.binding import Binding

from qlever.monitor_queries.widgets.footer import Footer


class CountingFooter(Footer):
    """Footer that counts its rebuilds."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rebuilds = 0

    async def recompose(self):
        self.rebuilds += 1
        await super().recompose()


class FooterApp(App):
    """Two bindings, one of which can be disabled dynamically."""

    BINDINGS = [
        Binding("a", "alpha", "Alpha"),
        Binding("b", "beta", "Beta"),
    ]

    def __init__(self):
        super().__init__()
        self.allow_beta = True

    def compose(self):
        yield CountingFooter(show_command_palette=False)

    def check_action(self, action, parameters):
        if action == "beta" and not self.allow_beta:
            return None
        return True


def disabled_keys(app):
    return [key.key for key in app.query("FooterKey") if key._disabled]


def test_redundant_refreshes_do_not_rebuild():
    async def run():
        app = FooterApp()
        async with app.run_test() as pilot:
            await pilot.pause()
            footer = app.query_one(CountingFooter)
            before = footer.rebuilds
            for _ in range(20):
                app.screen.refresh_bindings()
                await pilot.pause()
            assert footer.rebuilds == before

    asyncio.run(run())


def test_real_change_rebuilds():
    async def run():
        app = FooterApp()
        async with app.run_test() as pilot:
            await pilot.pause()
            footer = app.query_one(CountingFooter)
            before = footer.rebuilds
            app.allow_beta = False
            app.screen.refresh_bindings()
            await pilot.pause()
            assert footer.rebuilds > before
            assert disabled_keys(app) == ["b"]

    asyncio.run(run())


def test_change_while_unfocused_is_drawn_on_refocus():
    async def run():
        app = FooterApp()
        async with app.run_test() as pilot:
            await pilot.pause()
            # Disable the binding, then re-enable it while the terminal
            # is unfocused (the parent skips rebuilds then), then refocus.
            app.allow_beta = False
            app.screen.refresh_bindings()
            await pilot.pause()
            assert disabled_keys(app) == ["b"]
            app.post_message(events.AppBlur())
            await pilot.pause()
            app.allow_beta = True
            app.screen.refresh_bindings()
            await pilot.pause()
            app.post_message(events.AppFocus())
            await pilot.pause()
            await pilot.pause()
            assert disabled_keys(app) == []

    asyncio.run(run())
