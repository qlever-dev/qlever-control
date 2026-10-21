"""Tests for the wall-clock formatting shared by the widgets."""

from datetime import datetime

from qlever.monitor_queries.util import format_clock, format_range


def local_ms(*parts: int) -> int:
    """Epoch ms of a local date and time, so labels read the same anywhere."""
    return int(datetime(*parts).timestamp() * 1000)


MOMENT_MS = local_ms(2026, 10, 3, 14, 23, 17)
NOW_MS = local_ms(2026, 10, 5, 12, 0, 0)


def test_format_clock_defaults_to_time_with_seconds():
    assert format_clock(MOMENT_MS) == "14:23:17"


def test_format_clock_drops_the_seconds_when_asked():
    assert format_clock(MOMENT_MS, with_seconds=False) == "14:23"


def test_format_clock_dates_with_the_month_name_when_asked():
    assert format_clock(MOMENT_MS, with_date=True) == "Oct 3 14:23:17"


def test_format_clock_dates_and_drops_the_seconds_together():
    label = format_clock(MOMENT_MS, with_date=True, with_seconds=False)
    assert label == "Oct 3 14:23"


def test_format_range_of_today_shows_times_only():
    start_ms = local_ms(2026, 10, 5, 10, 2, 33)
    end_ms = local_ms(2026, 10, 5, 10, 17, 33)
    assert format_range(start_ms, end_ms, NOW_MS) == "10:02:33 → 10:17:33"


def test_format_range_of_a_past_day_shows_its_date_once():
    start_ms = local_ms(2026, 10, 4, 10, 2, 33)
    end_ms = local_ms(2026, 10, 4, 10, 17, 33)
    assert (
        format_range(start_ms, end_ms, NOW_MS) == "Oct 4 · 10:02:33 → 10:17:33"
    )


def test_format_range_across_midnight_dates_each_end():
    # The end is today, but the start is not, so each end gets its date.
    start_ms = local_ms(2026, 10, 4, 23, 30, 0)
    end_ms = local_ms(2026, 10, 5, 0, 30, 0)
    assert (
        format_range(start_ms, end_ms, NOW_MS)
        == "Oct 4 23:30:00 → Oct 5 00:30:00"
    )


def test_format_range_of_a_month_dates_each_end():
    start_ms = local_ms(2026, 9, 5, 12, 0, 0)
    assert (
        format_range(start_ms, NOW_MS, NOW_MS)
        == "Sep 5 12:00:00 → Oct 5 12:00:00"
    )
