import pytest

from qeval.oxigraph.commands.index_stats import IndexStatsCommand
from qlever.commands.benchmark_queries import compute_index_stats

# Oxigraph's own output, which comes before the times that `index` appends to
# the log. It also has seconds in it, which must not be taken for a phase.
PROGRESS = "1000 triples loaded in 1s (1000 triples/s)\n"

# The times that `index` appends after a build with an optimize step.
LOAD_AND_OPTIMIZE_TIMES = (
    "Load time: 100s\nOptimize time: 20s\nTOTAL time: 120s\n"
)

# The times that `index` appends without an optimize step, or when it failed.
LOAD_ONLY_TIMES = "Load time: 100s\nTOTAL time: 100s\n"


@pytest.mark.parametrize(
    "times,expected",
    [
        (
            LOAD_AND_OPTIMIZE_TIMES,
            [
                ("Load time", 100.0),
                ("Optimize time", 20.0),
                ("TOTAL time", 120.0),
            ],
        ),
        # Loading is the whole build, so only the total is shown.
        (LOAD_ONLY_TIMES, [("TOTAL time", 100.0)]),
        # The build did not finish.
        ("", []),
    ],
    ids=["load and optimize", "load only", "unfinished"],
)
def test_oxigraph_index_stats_parses_times_from_log(times, expected):
    durations = IndexStatsCommand().parse_index_durations(PROGRESS + times)

    assert list(durations.items()) == expected


def test_oxigraph_index_stats_gives_benchmark_total_time_and_size(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "olympics.index-log.txt").write_text(
        PROGRESS + LOAD_AND_OPTIMIZE_TIMES
    )
    index_dir = tmp_path / "olympics_index"
    index_dir.mkdir()
    (index_dir / "000001.sst").write_bytes(b"x" * 1000)
    (index_dir / "000002.sst").write_bytes(b"x" * 500)
    # A small RocksDB file next to the `.sst` files, which does not count.
    (index_dir / "CURRENT").write_bytes(b"x" * 16)

    assert compute_index_stats(IndexStatsCommand()) == (120.0, 1500)
