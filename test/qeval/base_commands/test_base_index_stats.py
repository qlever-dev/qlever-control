import pytest

from qlever.commands.benchmark_queries import compute_index_stats


@pytest.mark.parametrize(
    "time_unit,expected",
    [
        # Both phases in hours, picked to suit the longer one.
        ("auto", {"Parse": (30 / 3600, "h"), "Build": (2.0, "h")}),
        ("min", {"Parse": (0.5, "min"), "Build": (120.0, "min")}),
    ],
)
def test_base_index_stats_time_unit_suits_longest_phase(
    parse_args, fake_index_stats, tmp_path, time_unit, expected
):
    log_path = tmp_path / "olympics.index-log.txt"
    log_path.write_text("Parse 30\nBuild 7200\n")
    args = parse_args(
        fake_index_stats, ["--name", "olympics", "--time-unit", time_unit]
    )

    assert fake_index_stats.execute_time(args, log_path) == expected


def test_base_index_stats_adds_up_index_files(
    parse_args, fake_index_stats, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "olympics.fake-index.data").write_bytes(b"x" * 1000)
    (tmp_path / "olympics.fake-index.meta").write_bytes(b"x" * 500)
    # Not an index file of the engine, so it does not count.
    (tmp_path / "olympics.other").write_bytes(b"x" * 999)
    args = parse_args(fake_index_stats, ["--name", "olympics"])

    assert fake_index_stats.execute_space(args) == {"TOTAL size": (1500, "B")}


def test_base_index_stats_succeeds_with_log_and_index_files(
    parse_args, fake_index_stats, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "olympics.index-log.txt").write_text("Parse 30\n")
    (tmp_path / "olympics.fake-index.data").write_bytes(b"x" * 1000)
    args = parse_args(fake_index_stats, ["--name", "olympics"])

    assert fake_index_stats.execute(args)


def test_base_index_stats_fails_without_log(
    parse_args, fake_index_stats, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "olympics.fake-index.data").write_bytes(b"x" * 1000)
    args = parse_args(fake_index_stats, ["--name", "olympics"])

    assert not fake_index_stats.execute(args)


def test_base_index_stats_gives_benchmark_total_time_and_size(
    fake_index_stats, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "olympics.index-log.txt").write_text(
        "Load 60\nTOTAL time 90\n"
    )
    (tmp_path / "olympics.fake-index.data").write_bytes(b"x" * 1000)

    assert compute_index_stats(fake_index_stats) == (90.0, 1000)
