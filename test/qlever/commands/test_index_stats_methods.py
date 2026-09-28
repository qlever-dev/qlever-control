import pytest

from qlever.commands.index_stats import (
    compute_durations,
    compute_sizes,
    get_size_unit,
    get_time_unit,
)


@pytest.mark.parametrize("explicit_unit", ["s", "min", "h"])
@pytest.mark.parametrize("parse_duration", [None, 0, 50, 500, 5000])
def test_get_time_unit_explicit(explicit_unit, parse_duration):
    """Explicit time unit is returned as-is regardless of parse_duration."""
    assert get_time_unit(explicit_unit, parse_duration) == explicit_unit


@pytest.mark.parametrize(
    "parse_duration, expected_unit",
    [
        (None, "h"),
        (0, "s"),
        (199, "s"),
        (200, "min"),
        (3599, "min"),
        (3600, "h"),
        (10000, "h"),
    ],
)
def test_get_time_unit_auto(parse_duration, expected_unit):
    """Auto mode picks unit based on parse_duration thresholds."""
    assert get_time_unit("auto", parse_duration) == expected_unit


@pytest.mark.parametrize("explicit_unit", ["B", "MB", "GB", "TB"])
@pytest.mark.parametrize("total_size", [0, 500, int(1e7), int(1e13)])
def test_get_size_unit_explicit(explicit_unit, total_size):
    """Explicit size unit is returned as-is regardless of total_size."""
    assert get_size_unit(explicit_unit, total_size) == explicit_unit


@pytest.mark.parametrize(
    "total_size, expected_unit",
    [
        (0, "B"),
        (999_999, "B"),
        (1_000_000, "MB"),
        (999_999_999, "MB"),
        (1_000_000_000, "GB"),
        (999_999_999_999, "GB"),
        (1_000_000_000_000, "TB"),
        (5_000_000_000_000, "TB"),
    ],
)
def test_get_size_unit_auto(total_size, expected_unit):
    """Auto mode picks unit based on total_size thresholds."""
    assert get_size_unit("auto", total_size) == expected_unit


def test_compute_sizes_text_omitted_when_zero():
    """Text index entry is excluded from result when text index size is zero."""
    raw_sizes = {"index": 500, "vocabulary": 300, "text": 0, "total": 800}
    result = compute_sizes(raw_sizes, "B")
    assert "Files text.*" not in result
    assert list(result.keys()) == [
        "Files index.*",
        "Files vocabulary.*",
        "TOTAL size",
    ]


def test_compute_sizes_text_included_when_nonzero():
    """Text index entry is included in result when text index size is nonzero."""
    raw_sizes = {
        "index": 500,
        "vocabulary": 300,
        "text": 200,
        "total": 1000,
    }
    result = compute_sizes(raw_sizes, "B")
    assert "Files text.*" in result
    assert list(result.keys()) == [
        "Files index.*",
        "Files vocabulary.*",
        "Files text.*",
        "TOTAL size",
    ]


def test_compute_sizes_all_zero():
    """All sizes zero: auto resolves to 'B', text index is omitted."""
    raw_sizes = {"index": 0, "vocabulary": 0, "text": 0, "total": 0}
    result = compute_sizes(raw_sizes, "auto")
    assert result["Files index.*"] == (0, "B")
    assert result["Files vocabulary.*"] == (0, "B")
    assert result["TOTAL size"] == (0, "B")
    assert "Files text.*" not in result


@pytest.mark.parametrize(
    "size_unit, divisor",
    [("B", 1), ("MB", 1e6), ("GB", 1e9), ("TB", 1e12)],
)
def test_compute_sizes_conversion(size_unit, divisor):
    """Raw byte sizes are correctly divided by the unit factor."""
    raw_sizes = {
        "index": 5_000_000_000,
        "vocabulary": 1_000_000_000,
        "text": 500_000_000,
        "total": 6_500_000_000,
    }
    result = compute_sizes(raw_sizes, size_unit)
    assert result["Files index.*"] == (5_000_000_000 / divisor, size_unit)
    assert result["Files vocabulary.*"] == (1_000_000_000 / divisor, size_unit)
    assert result["Files text.*"] == (500_000_000 / divisor, size_unit)
    assert result["TOTAL size"] == (6_500_000_000 / divisor, size_unit)


def test_compute_sizes_auto_unit_propagated():
    """Auto-resolved unit is applied consistently to all entries."""
    raw_sizes = {
        "index": 2_000_000_000,
        "vocabulary": 500_000_000,
        "text": 100_000_000,
        "total": 2_600_000_000,
    }
    result = compute_sizes(raw_sizes, "auto")
    # total is 2.6e9 -> auto resolves to GB
    for _, (_, unit) in result.items():
        assert unit == "GB"


def log_line(time: str, message: str) -> str:
    """Build a timestamped log line matching the real log format."""
    return f"2025-01-15 {time}.000 - INFO: {message}\n"


# A complete log with all phases (new format)
COMPLETE_LOG_LINES = [
    log_line("10:00:00", "Processing triples from single input stream"),
    log_line("10:01:00", "Merging partial vocabularies ..."),
    log_line(
        "10:02:00", "Converting triples from local IDs to global IDs ..."
    ),
    log_line("10:03:00", "Creating permutations SPO and SOP ..."),
    log_line("10:05:00", "Creating permutations OSP and OPS ..."),
    log_line("10:07:00", "Creating permutations PSO and POS ..."),
    log_line("10:09:00", "Index build completed"),
]


def test_compute_durations_complete_build():
    """All phases present: every phase has a duration, TOTAL is computed."""
    result = compute_durations(COMPLETE_LOG_LINES, "s", False)
    assert result["Parse input"] == (60.0, "s")
    assert result["Build vocabularies"] == (60.0, "s")
    assert result["Convert to global IDs"] == (60.0, "s")
    assert result["Permutation SPO & SOP"] == (120.0, "s")
    assert result["Permutation OSP & OPS"] == (120.0, "s")
    assert result["Permutation PSO & POS"] == (120.0, "s")
    assert result["Text index"] == (None, "s")
    assert result["TOTAL time"] == (540.0, "s")


def test_compute_durations_empty_lines():
    """Empty input: no 'Processing' line found, returns empty dict."""
    result = compute_durations([], "s", False)
    assert result == {}


def test_compute_durations_only_processing():
    """Only 'Processing' line, no 'Merging': build still in first phase,
    returns empty dict."""
    lines = [
        log_line("10:00:00", "Processing triples from single input stream")
    ]
    result = compute_durations(lines, "s", False)
    assert result == {}


def test_compute_durations_partial_build():
    """Parse and merge done, but no convert or permutations yet: those
    phases have None durations."""
    lines = [
        log_line("10:00:00", "Processing triples from single input stream"),
        log_line("10:01:00", "Merging partial vocabularies ..."),
    ]
    result = compute_durations(lines, "s", False)
    assert result["Parse input"] == (60.0, "s")
    assert result["Build vocabularies"] == (None, "s")
    assert result["Convert to global IDs"] == (None, "s")
    assert result["Text index"] == (None, "s")
    assert "TOTAL time" not in result


def test_compute_durations_with_text_index():
    """Separate text index built after main build: TOTAL includes both."""
    lines = COMPLETE_LOG_LINES + [
        log_line("11:00:00", "Adding text index"),
        log_line("11:10:00", "Text index build completed"),
    ]
    result = compute_durations(lines, "s", False)
    assert result["Text index"] == (600.0, "s")
    # TOTAL = main build (540s) + text index (600s)
    assert result["TOTAL time"] == (540.0 + 600.0, "s")


def test_compute_durations_ignore_text_index():
    """ignore_text_index=True: text index duration is None, TOTAL excludes
    text time."""
    lines = COMPLETE_LOG_LINES + [
        log_line("11:00:00", "Adding text index"),
        log_line("11:10:00", "Text index build completed"),
    ]
    result = compute_durations(lines, "s", True)
    assert result["Text index"] == (None, "s")
    assert result["TOTAL time"] == (540.0, "s")


def test_compute_durations_old_log_format():
    """Old format uses 'Creating a pair' + 'Writing meta data for ...'
    instead of 'Creating permutations ...'."""
    lines = [
        log_line("10:00:00", "Processing triples from single input stream"),
        log_line("10:01:00", "Merging partial vocabularies ..."),
        log_line(
            "10:02:00", "Converting triples from local IDs to global IDs ..."
        ),
        log_line("10:03:00", "Creating a pair of permutations ..."),
        log_line("10:03:30", "Writing meta data for SPO and SOP ..."),
        log_line("10:05:00", "Creating a pair of permutations ..."),
        log_line("10:05:30", "Writing meta data for OSP and OPS ..."),
        log_line("10:07:00", "Index build completed"),
    ]
    result = compute_durations(lines, "s", False)
    assert "Permutation SPO & SOP" in result
    assert "Permutation OSP & OPS" in result
    assert result["Permutation SPO & SOP"] == (120.0, "s")
    assert result["Permutation OSP & OPS"] == (120.0, "s")


def test_compute_durations_time_unit_conversion():
    """Explicit time unit 'min': all durations converted from seconds."""
    result = compute_durations(COMPLETE_LOG_LINES, "min", False)
    assert result["Parse input"] == (1.0, "min")
    assert result["TOTAL time"] == (9.0, "min")


def test_compute_durations_auto_time_unit():
    """Auto time unit resolved based on parse phase duration (60s < 200
    -> 's')."""
    result = compute_durations(COMPLETE_LOG_LINES, "auto", False)
    # Parse phase is 60s which is < 200, so auto resolves to "s"
    for _, (_, unit) in result.items():
        assert unit == "s"


def test_compute_durations_no_index_build_completed():
    """Missing 'Index build completed' line: last permutation end and
    TOTAL are None."""
    lines = COMPLETE_LOG_LINES[:-1]
    result = compute_durations(lines, "s", False)
    assert result["Permutation SPO & SOP"] == (120.0, "s")
    assert result["Permutation OSP & OPS"] == (120.0, "s")
    assert result["Permutation PSO & POS"] == (None, "s")
    assert "TOTAL time" not in result
