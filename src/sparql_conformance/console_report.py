"""Console output helpers for the SPARQL conformance runner.

This module only uses the standard library and the Status enum. Status is
imported with a fallback because its path differs between the standalone tool
and the qlever-control integration.

Status is a str Enum, so comparisons work with both enum values and serialized
status strings.

Output is opt-in and written to stdout. ANSI colors are used only for TTY output
unless NO_COLOR is set.
"""

import bz2
import json
import os
import sys

try:
    from src.test_object import Status
except ImportError:  # qlever-control integration uses a different package name
    from sparql_conformance.test_object import Status

# ANSI escape codes.
_RESET = "\033[0m"
_COLORS = {
    "green": "\033[32m",
    "red": "\033[31m",
    "yellow": "\033[33m",
    "dim": "\033[2m",
    "bold": "\033[1m",
}


def _use_color() -> bool:
    """Colour only when writing to a real terminal and NO_COLOR is unset."""
    if os.environ.get("NO_COLOR") is not None:
        return False
    return sys.stdout.isatty()


def _c(text: str, color: str) -> str:
    if not _use_color():
        return text
    return f"{_COLORS.get(color, '')}{text}{_RESET}"


def _status_value(status) -> str:
    """Return the plain status string for either an enum member or a str."""
    return getattr(status, "value", status) or ""


def test_line(test) -> None:
    """Print a single colored PASS/FAIL/INTENDED line for a finished test."""
    status = _status_value(test.status)
    error = _status_value(getattr(test, "error_type", ""))
    if status == Status.PASSED:
        label = _c("PASS", "green")
    elif status == Status.FAILED:
        label = _c("FAIL", "red")
    elif status == Status.INTENDED:
        label = _c("INTD", "yellow")
    else:
        label = _c("----", "dim")
    suffix = f"  ({error})" if error and status != Status.PASSED else ""
    print(f"  {label}  {test.name}{suffix}")


def print_summary(total_info: dict, suites_data: dict) -> None:
    """Print a colored totals block, per-suite and overall.

    ``total_info`` and each suite's ``info`` share the keys produced by
    TestSuite.build_results_dict: passed / tests / failed / passedFailed /
    notTested.
    """
    print()
    print(_c("=== Conformance summary ===", "bold"))
    for suite_key, suite in suites_data.items():
        _print_info_row(suite_key, suite.get("info", {}))
    if len(suites_data) != 1:
        _print_info_row("total", total_info)


def _print_info_row(label: str, info: dict) -> None:
    passed = info.get("passed", 0)
    failed = info.get("failed", 0)
    intended = info.get("passedFailed", 0)
    not_tested = info.get("notTested", 0)
    total = info.get("tests", 0)
    parts = [
        _c(f"{passed} passed", "green"),
        _c(f"{failed} failed", "red"),
        _c(f"{intended} intended", "yellow"),
        _c(f"{not_tested} not tested", "dim"),
    ]
    print(f"  {label:<12} {' / '.join(parts)}  (of {total})")


def print_failures(suites_data: dict) -> None:
    """Print a concise list of the failed tests (name + error type)."""
    failures = []
    for suite_key, suite in suites_data.items():
        for name, test in suite.get("tests", {}).items():
            if test.get("status") == Status.FAILED:
                failures.append((suite_key, name, test.get("errorType", "")))
    if not failures:
        print(_c("No failures.", "green"))
        return
    print()
    print(_c(f"Failed tests ({len(failures)}):", "red"))
    for suite_key, name, error in failures:
        detail = f"  [{suite_key}] {name}"
        if error:
            detail += f"  ({error})"
        print(_c(detail, "red"))


def read_json_bz2(path: str) -> dict:
    """Load a results file written by TestSuite.compress_json_bz2."""
    with bz2.BZ2File(path, "r") as raw_file:
        return json.load(raw_file)


def compare_runs(baseline: dict, current: dict) -> dict:
    """Diff two v2 result documents by per-test status.

    Returns {"regressions": [...], "fixes": [...]} where each entry is
    (suite_key, test_name, baseline_status, current_status).  A regression is a
    test that passed (or was an intended deviation) in the baseline and now
    fails; a fix is the reverse.
    """
    regressions = []
    fixes = []
    base_suites = baseline.get("suites", {})
    for suite_key, suite in current.get("suites", {}).items():
        base_tests = base_suites.get(suite_key, {}).get("tests", {})
        for name, test in suite.get("tests", {}).items():
            if name not in base_tests:
                continue
            cur = test.get("status")
            base = base_tests[name].get("status")
            if base in (Status.PASSED, Status.INTENDED) and cur == Status.FAILED:
                regressions.append((suite_key, name, base, cur))
            elif base == Status.FAILED and cur in (Status.PASSED, Status.INTENDED):
                fixes.append((suite_key, name, base, cur))
    return {"regressions": regressions, "fixes": fixes}


def print_comparison(diff: dict) -> None:
    """Print regressions (red) and fixes (green) from compare_runs."""
    regressions = diff.get("regressions", [])
    fixes = diff.get("fixes", [])
    print()
    print(_c("=== Comparison to baseline ===", "bold"))
    print(
        f"  {_c(str(len(regressions)) + ' regressions', 'red')}"
        f"   {_c(str(len(fixes)) + ' fixes', 'green')}"
    )
    if regressions:
        print(_c("Regressions (now failing):", "red"))
        for suite_key, name, base, cur in regressions:
            print(_c(f"  [{suite_key}] {name}  ({base} -> {cur})", "red"))
    if fixes:
        print(_c("Fixes (now passing):", "green"))
        for suite_key, name, base, cur in fixes:
            print(_c(f"  [{suite_key}] {name}  ({base} -> {cur})", "green"))
