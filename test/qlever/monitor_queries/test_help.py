"""Tests for the help-row text built from a screen's active bindings."""

from textual.binding import ActiveBinding, Binding

from qlever.monitor_queries.util import help_text


def key_display(binding: Binding) -> str:
    """Stand-in for the app's key formatter."""
    return binding.key_display or binding.key


def active(
    key: str,
    action: str,
    description: str,
    display: str | None = None,
    enabled: bool = True,
) -> ActiveBinding:
    """Build one entry of a screen's active_bindings map."""
    return ActiveBinding(
        node=None,
        binding=Binding(
            key=key,
            action=action,
            description=description,
            key_display=display,
        ),
        enabled=enabled,
        tooltip="",
    )


def test_renders_pairs_in_the_given_order():
    bindings = {
        "i": active(key="i", action="invert_sort", description="Invert sort"),
        "f": active(key="f", action="edit_filter", description="Filter"),
    }
    text = help_text(bindings, ["edit_filter", "invert_sort"], key_display)
    assert text == (
        "[$text on $success] f [/] Filter"
        "   [$text on $success] i [/] Invert sort"
    )


def test_uses_the_bindings_own_key_display():
    bindings = {
        "less_than_sign": active(
            key="less_than_sign",
            action="sort_prev_column",
            description="Sort column",
            display="< >",
        )
    }
    text = help_text(bindings, ["sort_prev_column"], key_display)
    assert text == "[$text on $success] < > [/] Sort column"


def test_follows_a_remapped_key():
    bindings = {
        "o": active(
            key="o", action="sort_prev_column", description="Sort column"
        )
    }
    text = help_text(bindings, ["sort_prev_column"], key_display)
    assert text == "[$text on $success] o [/] Sort column"


def test_skips_an_action_that_is_not_bound():
    bindings = {
        "f": active(key="f", action="edit_filter", description="Filter")
    }
    text = help_text(bindings, ["edit_filter", "show_plot"], key_display)
    assert text == "[$text on $success] f [/] Filter"


def test_skips_a_disabled_action():
    bindings = {
        "f": active(key="f", action="edit_filter", description="Filter"),
        "F": active(
            key="F",
            action="clear_filters",
            description="Clear filters",
            enabled=False,
        ),
    }
    text = help_text(bindings, ["edit_filter", "clear_filters"], key_display)
    assert text == "[$text on $success] f [/] Filter"


def test_no_listed_action_is_bound():
    assert help_text({}, ["edit_filter", "invert_sort"], key_display) == ""
