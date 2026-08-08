from types import SimpleNamespace

import pytest

# The plot extra (numpy, matplotlib) is optional, so skip this whole
# module when it is not installed instead of failing to import.
np = pytest.importorskip("numpy")
pytest.importorskip("matplotlib")

from qlever.resource_usage.usage_plot import (  # noqa: E402
    UsagePlot,
    build_plot_subtitle,
    compute_phase_boundaries,
    downsample_for_plot,
    pick_time_unit,
    read_usage_tsv,
)


def write_log(tmp_path, lines):
    """Write an index-log file from full timestamped lines."""
    path = tmp_path / "index-log.txt"
    path.write_text("\n".join(lines) + "\n")
    return path


@pytest.mark.parametrize(
    "seconds,expected",
    [
        (0, ("Elapsed (s)", 1.0)),
        (199.9, ("Elapsed (s)", 1.0)),
        # Boundary: 200 leaves seconds for minutes.
        (200, ("Elapsed (min)", 60.0)),
        (3599, ("Elapsed (min)", 60.0)),
        # Boundary: 3600 leaves minutes for hours.
        (3600, ("Elapsed (h)", 3600.0)),
        (10000, ("Elapsed (h)", 3600.0)),
    ],
)
def test_pick_time_unit(seconds, expected):
    assert pick_time_unit(seconds) == expected


@pytest.mark.parametrize(
    "triples,expected_fragment",
    [
        (1_000_000, "batch = 1M triples"),
        (5_000_000, "batch = 5M triples"),
        (2_500_000, "batch = 2.5M triples"),
        (1_000, "batch = 1K triples"),
        (50_000, "batch = 50K triples"),
        (500, "batch = 500 triples"),
    ],
)
def test_build_plot_subtitle_batch_formatting(
    triples, expected_fragment, tmp_path
):
    log_path = tmp_path / "index-log.txt"
    log_path.write_text("")
    settings_json = f'{{"num-triples-per-batch": {triples}}}'
    subtitle = build_plot_subtitle(log_path, "", settings_json)
    assert subtitle == expected_fragment


def test_build_plot_subtitle_stxxl(tmp_path):
    log_path = tmp_path / "index-log.txt"
    log_path.write_text("")
    subtitle = build_plot_subtitle(log_path, "5G", "{}")
    assert subtitle == "STXXL = 5G"


def test_build_plot_subtitle_no_values(tmp_path):
    log_path = tmp_path / "index-log.txt"
    log_path.write_text("")
    assert build_plot_subtitle(log_path, "", "{}") is None


def test_build_plot_subtitle_invalid_settings_json(tmp_path):
    log_path = tmp_path / "index-log.txt"
    log_path.write_text("")
    # Malformed JSON must not raise; the batch part is simply omitted.
    assert build_plot_subtitle(log_path, "", "not-json") is None


def test_read_usage_tsv_parses_columns_and_blank_as_nan(tmp_path):
    path = tmp_path / "data.resource-usage-log.tsv"
    path.write_text("elapsed_s\trss\tcpu_percent\n1.0\t100\t5.0\n2.0\t\t6.0\n")
    data = read_usage_tsv(path)
    assert set(data) == {"elapsed_s", "rss", "cpu_percent"}
    np.testing.assert_array_equal(data["elapsed_s"], np.array([1.0, 2.0]))
    assert data["rss"][0] == 100
    assert np.isnan(data["rss"][1])
    np.testing.assert_array_equal(data["cpu_percent"], np.array([5.0, 6.0]))


def test_read_usage_tsv_header_only(tmp_path):
    path = tmp_path / "data.tsv"
    path.write_text("elapsed_s\trss\tcpu_percent\n")
    data = read_usage_tsv(path)
    assert len(data["elapsed_s"]) == 0


def test_read_usage_tsv_empty_file(tmp_path):
    path = tmp_path / "data.tsv"
    path.write_text("")
    assert read_usage_tsv(path) == {}


def test_downsample_for_plot_returns_input_when_within_budget():
    data = {
        "elapsed_s": np.arange(5.0),
        "rss": np.arange(5.0),
        "cpu_percent": np.arange(5.0),
    }
    assert downsample_for_plot(data, 10) is data


def test_downsample_for_plot_caps_points_and_keeps_peak():
    data = {
        "elapsed_s": np.arange(10.0),
        "rss": np.array([1, 2, 3, 99, 4, 5, 6, 7, 8, 9], dtype=float),
        "cpu_percent": np.zeros(10),
    }
    out = downsample_for_plot(data, 3)
    assert len(out["elapsed_s"]) <= 3
    # nanmax bucketing must preserve the global peak exactly.
    assert np.nanmax(out["rss"]) == 99
    # elapsed_s uses nanmin, so the first bucket starts at the origin.
    assert out["elapsed_s"][0] == 0.0


def test_compute_phase_boundaries_missing_file(tmp_path):
    phases = compute_phase_boundaries(tmp_path / "nope.txt")
    assert phases == {}


def test_compute_phase_boundaries_no_processing_line(tmp_path):
    path = write_log(tmp_path, ["2026-06-01 10:00:00 - INFO: Something else"])
    phases = compute_phase_boundaries(path)
    assert phases == {}


def test_compute_phase_boundaries_with_permutations(tmp_path):
    path = write_log(
        tmp_path,
        [
            "2026-06-01 10:00:00 - INFO: Processing input",
            "2026-06-01 10:00:10 - INFO: Merging partial vocabularies",
            "2026-06-01 10:00:20 - INFO: Converting triples",
            "2026-06-01 10:00:30 - INFO: Creating permutations PSO and POS",
            "2026-06-01 10:00:40 - INFO: Creating permutations SPO and SOP",
            "2026-06-01 10:00:50 - INFO: Index build completed",
        ],
    )
    phases = compute_phase_boundaries(path)
    assert phases == {
        "Parse input": (0.0, 10.0),
        "Build vocabularies": (10.0, 20.0),
        "Convert to global IDs": (20.0, 30.0),
        "Permutation PSO & POS": (30.0, 40.0),
        # The last permutation ends at "Index build completed".
        "Permutation SPO & SOP": (40.0, 50.0),
    }


def test_compute_phase_boundaries_without_permutations(tmp_path):
    path = write_log(
        tmp_path,
        [
            "2026-06-01 10:00:00 - INFO: Processing input",
            "2026-06-01 10:00:10 - INFO: Merging partial vocabularies",
            "2026-06-01 10:00:20 - INFO: Converting triples",
            "2026-06-01 10:00:50 - INFO: Index build completed",
        ],
    )
    phases = compute_phase_boundaries(path)
    assert phases == {
        "Parse input": (0.0, 10.0),
        "Build vocabularies": (10.0, 20.0),
        # No permutations: convert runs through to build completion.
        "Convert to global IDs": (20.0, 50.0),
    }


def test_compute_phase_boundaries_dedups_repeated_permutation_names(tmp_path):
    path = write_log(
        tmp_path,
        [
            "2026-06-01 10:00:00 - INFO: Processing input",
            "2026-06-01 10:00:10 - INFO: Merging partial vocabularies",
            "2026-06-01 10:00:20 - INFO: Converting triples",
            "2026-06-01 10:00:30 - INFO: Creating permutations PSO and POS",
            "2026-06-01 10:00:40 - INFO: Creating permutations PSO and POS",
            "2026-06-01 10:00:50 - INFO: Index build completed",
        ],
    )
    phases = compute_phase_boundaries(path)
    assert "Permutation PSO & POS" in phases
    assert "Permutation PSO & POS (2)" in phases
    assert phases["Permutation PSO & POS"] == (30.0, 40.0)
    assert phases["Permutation PSO & POS (2)"] == (40.0, 50.0)


def test_compute_phase_boundaries_skips_incomplete_phase(tmp_path):
    # Only the "Processing" line: the start is found, but no later
    # timestamps, so every phase has a None endpoint and is skipped.
    path = write_log(
        tmp_path, ["2026-06-01 10:00:00 - INFO: Processing input"]
    )
    phases = compute_phase_boundaries(path)
    assert phases == {}


def test_render_usage_plot_missing_tsv(tmp_path):
    assert UsagePlot("missing", None, output_dir=tmp_path).render() is None


def test_render_usage_plot_header_only_tsv_renders_nothing(tmp_path):
    # A TSV with only the header (no samples) must not report success
    # or leave a PNG behind.
    tsv_path = tmp_path / "data.index.resource-usage-log.tsv"
    tsv_path.write_text("elapsed_s\trss\tcpu_percent\n")
    assert UsagePlot("data", None, output_dir=tmp_path).render() is None
    assert not (tmp_path / "data.resource-usage-plot.png").exists()


def test_render_usage_plot_falls_back_to_old_tsv_name(tmp_path):
    # Older qlever versions wrote the log without the `.index.` part.
    tsv_path = tmp_path / "data.resource-usage-log.tsv"
    tsv_path.write_text(
        "elapsed_s\trss\tcpu_percent\n1.0\t100\t5.0\n2.0\t200\t6.0\n"
    )
    args = SimpleNamespace(stxxl_memory="", settings_json="{}")
    plot_path = UsagePlot("data", args, output_dir=tmp_path).render()
    assert plot_path == tmp_path / "data.resource-usage-plot.png"
    assert plot_path.exists()
